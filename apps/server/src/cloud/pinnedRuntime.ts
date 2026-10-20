import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Semaphore from "effect/Semaphore";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";

import {
  CLI_RELEASE_CHECKSUMS_FILE,
  cliArchiveFileName,
  cliArchivePlatformKey,
  cliArchiveTarCommand,
  cliReleaseDownloadBaseUrl,
  parseChecksums,
} from "@t3tools/shared/cliRelease";

import * as HostProcess from "@t3tools/shared/HostProcess";

import * as ProcessRunner from "../processRunner.ts";
import {
  compareExactServiceVersions,
  isExactServiceVersion,
  parseServiceState,
  SERVICE_STATE_FILE,
} from "./serviceProtocol.ts";

const PINNED_RUNTIME_DIR = "runtime";
const PINNED_RUNTIME_INSTALL_TIMEOUT = Duration.minutes(10);
const PINNED_RUNTIME_DOWNLOAD_STALL_TIMEOUT = Duration.minutes(1);
const PINNED_RUNTIME_ARCHIVE_FILE = "t3-runtime-archive";
// Boot-service setup and remote update can construct separate layers. Serialize
// the complete install transaction across every caller in this process.
const pinnedRuntimeInstallLock = Semaphore.makeUnsafe(1);

export interface PinnedRuntimePaths {
  readonly versionDir: string;
  readonly entryPath: string;
  readonly sentinelPath: string;
}

export function pinnedRuntimeCommand(paths: PinnedRuntimePaths): {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
} {
  return { command: paths.entryPath, args: [] };
}

export function pinnedRuntimeVersionsDir(path: Path.Path, baseDir: string): string {
  return path.join(baseDir, PINNED_RUNTIME_DIR, "versions");
}

export function pinnedRuntimePaths(
  path: Path.Path,
  baseDir: string,
  version: string,
  platform: NodeJS.Platform,
): PinnedRuntimePaths {
  const versionDir = path.join(pinnedRuntimeVersionsDir(path, baseDir), version);
  return {
    versionDir,
    entryPath: path.join(versionDir, platform === "win32" ? "t3.exe" : "t3"),
    sentinelPath: path.join(versionDir, ".install-complete"),
  };
}

export class PinnedRuntimeInstallError extends Schema.TaggedError<PinnedRuntimeInstallError>()(
  "PinnedRuntimeInstallError",
  {
    step: Schema.String,
    exitCode: Schema.optional(Schema.Number),
    stdoutLength: Schema.optional(Schema.Number),
    stderrLength: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.exitCode === undefined
      ? `Pinned runtime install failed while ${this.step}.`
      : `Pinned runtime install failed while ${this.step} (exit code ${this.exitCode}).`;
  }
}

export class PinnedRuntimePreflightBlockedError extends Schema.TaggedError<PinnedRuntimePreflightBlockedError>()(
  "PinnedRuntimePreflightBlockedError",
  {
    version: Schema.String,
    reason: Schema.String,
  },
) {
  override get message(): string {
    return this.reason;
  }
}

export type PinnedRuntimeProgress =
  | { readonly stage: "download"; readonly received: number; readonly total: number | undefined }
  | { readonly stage: "verify" | "extract" | "validate" | "cached" };

/**
 * The sentinel is written only after extraction and validation succeed; checking the entry file
 * alone is not enough, since tar writes the executable before the last native package and a
 * killed install leaves a plausible-looking but broken tree behind.
 */

interface PinnedRuntimeInstallInput {
  readonly baseDir: string;
  readonly version: string;
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly runner: ProcessRunner.ProcessRunner["Service"];
  readonly validate: (
    paths: PinnedRuntimePaths,
  ) => Effect.Effect<void, PinnedRuntimeInstallError | PinnedRuntimePreflightBlockedError>;
  readonly platform: NodeJS.Platform;
  readonly arch: string;
  readonly httpClient: HttpClient.HttpClient;
  readonly releaseBaseUrl?: string | undefined;
  readonly onProgress?: (progress: PinnedRuntimeProgress) => void;
}

const fetchReleaseAsset = Effect.fn("cloud.pinned_runtime.fetch_release_asset")(function* (
  httpClient: HttpClient.HttpClient,
  url: string,
  step: string,
  onProgress?: (progress: PinnedRuntimeProgress) => void,
) {
  // Bound stalls while holding the install lock, but allow transfers that keep
  // receiving bytes to finish regardless of their total duration.
  return yield* httpClient.execute(HttpClientRequest.get(url)).pipe(
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.timeout(PINNED_RUNTIME_DOWNLOAD_STALL_TIMEOUT),
    Effect.flatMap(
      Effect.fn(function* (response) {
        const length = Number(response.headers["content-length"]);
        const total = Number.isFinite(length) && length > 0 ? length : undefined;
        let received = 0;
        onProgress?.({ stage: "download", received, total });
        const chunks = yield* response.stream.pipe(
          Stream.timeoutOrElse({
            duration: PINNED_RUNTIME_DOWNLOAD_STALL_TIMEOUT,
            orElse: () => Stream.fail(new Cause.TimeoutError()),
          }),
          Stream.tap((chunk) =>
            Effect.sync(() => {
              received += chunk.byteLength;
              onProgress?.({ stage: "download", received, total });
            }),
          ),
          Stream.runCollect,
        );
        if (total === undefined && received > 0) {
          onProgress?.({ stage: "download", received, total: received });
        }
        const bytes = new Uint8Array(received);
        let offset = 0;
        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.byteLength;
        }
        return bytes;
      }),
    ),
    Effect.mapError(
      (cause) =>
        new PinnedRuntimeInstallError({
          step: cause._tag === "TimeoutError" ? `${step} (timed out)` : step,
          cause,
        }),
    ),
  );
});

const installFromArchive = Effect.fn("cloud.pinned_runtime.install_archive")(function* (
  input: PinnedRuntimeInstallInput,
  stagingDir: string,
) {
  const { fs, path } = input;
  const platformKey = cliArchivePlatformKey(input.platform, input.arch);
  if (platformKey === undefined) {
    return yield* new PinnedRuntimeInstallError({
      step: `selecting a t3 release archive for ${input.platform}-${input.arch}`,
    });
  }
  const httpClient = input.httpClient;
  const baseUrl = cliReleaseDownloadBaseUrl(input.version, input.releaseBaseUrl);
  const fileName = cliArchiveFileName(input.version, platformKey);

  input.onProgress?.({ stage: "download", received: 0, total: undefined });
  const checksums = parseChecksums(
    new TextDecoder().decode(
      yield* fetchReleaseAsset(
        httpClient,
        `${baseUrl}/${CLI_RELEASE_CHECKSUMS_FILE}`,
        "downloading the t3 release checksums",
      ),
    ),
  );
  const expected = checksums.get(fileName);
  if (expected === undefined) {
    return yield* new PinnedRuntimeInstallError({
      step: `finding ${fileName} in the t3 release checksums`,
    });
  }
  const archive = yield* fetchReleaseAsset(
    httpClient,
    `${baseUrl}/${fileName}`,
    "downloading the t3 release archive",
    input.onProgress,
  );
  input.onProgress?.({ stage: "verify" });
  const digest = yield* Effect.tryPromise({
    try: () => crypto.subtle.digest("SHA-256", archive),
    catch: (cause) =>
      new PinnedRuntimeInstallError({ step: "verifying the t3 release archive", cause }),
  });
  if (Hex.encode(new Uint8Array(digest)) !== expected) {
    return yield* new PinnedRuntimeInstallError({
      step: "verifying the t3 release archive checksum",
    });
  }

  const archivePath = path.join(stagingDir, PINNED_RUNTIME_ARCHIVE_FILE);
  yield* fs
    .writeFile(archivePath, archive)
    .pipe(
      Effect.mapError(
        (cause) => new PinnedRuntimeInstallError({ step: "writing the t3 release archive", cause }),
      ),
    );
  input.onProgress?.({ stage: "extract" });
  const extractStep = "extracting the t3 release archive";
  // The archive wraps everything in one directory named after its stem;
  // strip it so the executable lands at <versionDir>/t3.
  yield* input.runner
    .run({
      command: cliArchiveTarCommand(input.platform, process.env),
      args: ["-xf", archivePath, "-C", stagingDir, "--strip-components=1"],
      timeout: PINNED_RUNTIME_INSTALL_TIMEOUT,
    })
    .pipe(
      Effect.mapError((cause) => new PinnedRuntimeInstallError({ step: extractStep, cause })),
      Effect.filterOrFail(
        (result) => result.code === 0,
        (result) =>
          new PinnedRuntimeInstallError({
            step: extractStep,
            exitCode: Number(result.code),
            stdoutLength: result.stdout.length,
            stderrLength: result.stderr.length,
          }),
      ),
    );
  yield* fs.remove(archivePath, { force: true }).pipe(Effect.ignore);
});

const installPinnedRuntime = Effect.fn("cloud.pinned_runtime.ensure_installed")(function* (
  input: PinnedRuntimeInstallInput,
) {
  const { fs } = input;
  const paths = pinnedRuntimePaths(input.path, input.baseDir, input.version, input.platform);
  const [versionDirExists, entryExists, sentinel] = yield* Effect.all([
    fs.exists(paths.versionDir),
    fs.exists(paths.entryPath),
    fs.readFileString(paths.sentinelPath).pipe(Effect.option),
  ]).pipe(
    Effect.mapError(
      (cause) => new PinnedRuntimeInstallError({ step: "checking the pinned runtime", cause }),
    ),
  );
  const alreadyPinned =
    entryExists && Option.isSome(sentinel) && sentinel.value.trim() === input.version;
  if (alreadyPinned) {
    input.onProgress?.({ stage: "cached" });
    yield* input.validate(paths);
    return paths;
  }
  if (versionDirExists) {
    yield* fs.remove(paths.versionDir, { recursive: true, force: true }).pipe(
      Effect.mapError(
        (cause) =>
          new PinnedRuntimeInstallError({
            step: "removing an incomplete pinned runtime",
            cause,
          }),
      ),
    );
  }

  const versionsDir = input.path.dirname(paths.versionDir);
  yield* fs.makeDirectory(versionsDir, { recursive: true }).pipe(
    Effect.mapError(
      (cause) =>
        new PinnedRuntimeInstallError({
          step: "preparing the pinned runtime directory",
          cause,
        }),
    ),
  );
  const stagingDir = yield* fs
    .makeTempDirectory({
      directory: versionsDir,
      prefix: ".staging-",
    })
    .pipe(
      Effect.mapError(
        (cause) =>
          new PinnedRuntimeInstallError({
            step: "preparing the pinned runtime directory",
            cause,
          }),
      ),
    );
  const stagingPaths: PinnedRuntimePaths = {
    versionDir: stagingDir,
    entryPath: input.path.join(stagingDir, input.path.relative(paths.versionDir, paths.entryPath)),
    sentinelPath: input.path.join(stagingDir, ".install-complete"),
  };

  return yield* Effect.gen(function* () {
    yield* installFromArchive(input, stagingDir);

    input.onProgress?.({ stage: "validate" });
    yield* input.validate(stagingPaths);
    yield* fs
      .writeFileString(stagingPaths.sentinelPath, `${input.version}\n`)
      .pipe(
        Effect.mapError(
          (cause) =>
            new PinnedRuntimeInstallError({ step: "recording the completed install", cause }),
        ),
      );
    const published = yield* fs.rename(stagingDir, paths.versionDir).pipe(
      Effect.as(true),
      Effect.catch((cause) =>
        Effect.all([
          fs.exists(paths.entryPath),
          fs.readFileString(paths.sentinelPath).pipe(Effect.option),
        ]).pipe(
          Effect.mapError(
            (checkCause) =>
              new PinnedRuntimeInstallError({
                step: "checking a concurrently published pinned runtime",
                cause: checkCause,
              }),
          ),
          Effect.flatMap(([publishedEntryExists, publishedSentinel]) =>
            publishedEntryExists &&
            Option.isSome(publishedSentinel) &&
            publishedSentinel.value.trim() === input.version
              ? Effect.succeed(false)
              : Effect.fail(
                  new PinnedRuntimeInstallError({
                    step: "publishing the pinned runtime",
                    cause,
                  }),
                ),
          ),
        ),
      ),
    );
    if (!published) yield* input.validate(paths);
    return paths;
  }).pipe(
    Effect.ensuring(fs.remove(stagingDir, { recursive: true, force: true }).pipe(Effect.ignore)),
  );
});

export const ensurePinnedRuntimeInstalled = (input: PinnedRuntimeInstallInput) =>
  pinnedRuntimeInstallLock.withPermit(installPinnedRuntime(input));

const PINNED_RUNTIME_PRUNED_PREFIX = ".pruned-";
/**
 * One `t3` launcher path per line, appended by the install scripts. A service does not see the
 * installer's `T3CODE_INSTALL_BIN_DIR` or interactive PATH, so this is how it finds a custom one.
 */
export const PINNED_RUNTIME_LAUNCHERS_FILE = "launchers";

export function launcherOwnsVersionsDir(
  path: Path.Path,
  versionsDir: string,
  candidate: string,
): boolean {
  const relative = path.relative(versionsDir, path.resolve(candidate));
  return relative.length > 0 && !relative.startsWith("..") && !path.isAbsolute(relative);
}

/** The executable a `t3.cmd` shim written by the install script or `t3 update` runs. */
export const windowsShimTarget = (contents: string) => /^"([^"]+)"/m.exec(contents)?.[1];

/** Versions the `t3` launchers written by the install scripts and `t3 update` point at. */
const shellLauncherVersions = Effect.fn("cloud.pinned_runtime.shell_launcher_versions")(function* (
  baseDir: string,
  versionsDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const environment = yield* HostProcess.Environment;
  const home = yield* HostProcess.HomeDirectory;
  const launcherName = platform === "win32" ? "t3.cmd" : "t3";
  const recorded = yield* fs
    .readFileString(path.join(baseDir, PINNED_RUNTIME_DIR, PINNED_RUNTIME_LAUNCHERS_FILE))
    .pipe(Effect.orElseSucceed(() => ""));
  const launchers = [
    ...recorded.split(/\r?\n/),
    ...[
      environment["T3CODE_INSTALL_BIN_DIR"] || path.join(home, ".local", "bin"),
      ...(environment["PATH"] ?? environment["Path"] ?? "").split(platform === "win32" ? ";" : ":"),
    ].map((directory) => (directory.trim().length > 0 ? path.join(directory, launcherName) : "")),
  ].filter((launcher) => launcher.trim().length > 0);
  const versions = new Set<string>();
  for (const launcher of launchers) {
    const target =
      platform === "win32"
        ? yield* fs.readFileString(launcher).pipe(
            Effect.map(windowsShimTarget),
            Effect.orElseSucceed(() => undefined),
          )
        : yield* fs.readLink(launcher).pipe(
            Effect.map((link) => path.resolve(path.dirname(launcher), link)),
            Effect.orElseSucceed(() => undefined),
          );
    if (target === undefined || !launcherOwnsVersionsDir(path, versionsDir, target)) continue;
    versions.add(path.relative(versionsDir, path.resolve(target)).split(path.sep)[0]!);
  }
  return versions;
});

/**
 * Command lines show argv, so this only sees runtimes started by absolute path: the service
 * launcher and its children, and anything run through a `t3.cmd` shim. A server started through
 * the `t3` symlink shows up as `t3 serve`; callers keep its version from the server runtime state.
 */
const runningCommandLines = Effect.fn("cloud.pinned_runtime.running_command_lines")(function* () {
  const runner = yield* ProcessRunner.ProcessRunner;
  const platform = yield* HostProcess.Platform;
  const result = yield* runner
    .run({
      ...(platform === "win32"
        ? {
            command: "powershell.exe",
            args: [
              "-NoProfile",
              "-NonInteractive",
              "-Command",
              "Get-CimInstance Win32_Process | Select-Object -ExpandProperty CommandLine",
            ],
          }
        : { command: "ps", args: ["-ax", "-o", "command="] }),
      timeout: Duration.seconds(10),
      maxOutputBytes: 16 * 1024 * 1024,
    })
    .pipe(Effect.option);
  return Option.isSome(result) && result.value.code === 0 && result.value.stdout.length > 0
    ? Option.some(result.value.stdout)
    : Option.none();
});

const selectUnusedRuntimes = Effect.fn("cloud.pinned_runtime.select_unused")(function* (input: {
  readonly baseDir: string;
  readonly keep: ReadonlyArray<string>;
  readonly references: ReadonlyArray<string>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const versionsDir = pinnedRuntimeVersionsDir(path, input.baseDir);
  const entries = yield* fs.readDirectory(versionsDir).pipe(Effect.orElseSucceed(() => []));
  // An interrupted prune has already renamed its runtime out of the way.
  const pruned = entries
    .filter((entry) => entry.startsWith(PINNED_RUNTIME_PRUNED_PREFIX))
    .map((entry) => path.join(versionsDir, entry));

  const completed: Array<string> = [];
  for (const version of entries) {
    if (!isExactServiceVersion(version)) continue;
    const versionDir = path.join(versionsDir, version);
    const isLink = yield* fs.readLink(versionDir).pipe(Effect.isSuccess);
    const sentinel = yield* fs
      .readFileString(path.join(versionDir, ".install-complete"))
      .pipe(Effect.option);
    if (!isLink && Option.isSome(sentinel) && sentinel.value.trim() === version) {
      completed.push(version);
    }
  }
  if (completed.length === 0) return pruned;

  const keep = new Set(input.keep);
  const stateText = yield* fs
    .readFileString(path.join(input.baseDir, PINNED_RUNTIME_DIR, SERVICE_STATE_FILE))
    .pipe(Effect.option);
  if (Option.isSome(stateText)) {
    const state = parseServiceState(stateText.value);
    // State this version cannot read may reference runtimes in ways it does not know.
    if (state === undefined) return pruned;
    keep.add(state.activeVersion);
    if (state.update !== undefined) {
      keep.add(state.update.fromVersion);
      keep.add(state.update.targetVersion);
    }
  }
  for (const version of yield* shellLauncherVersions(input.baseDir, versionsDir)) {
    keep.add(version);
  }
  const commandLines = yield* runningCommandLines();
  // Without a process list, a runtime in use would look unused.
  if (Option.isNone(commandLines)) return pruned;
  const references = [commandLines.value, ...input.references];

  for (const version of completed) {
    const versionDir = path.join(versionsDir, version);
    if (
      keep.has(version) ||
      // Another process may be installing a newer runtime, so only older ones go.
      !input.keep.every((kept) => compareExactServiceVersions(version, kept) < 0) ||
      references.some((reference) => reference.includes(versionDir + path.sep))
    ) {
      continue;
    }
    const target = path.join(versionsDir, `${PINNED_RUNTIME_PRUNED_PREFIX}${version}`);
    // Renaming first means an interrupted removal never leaves a partial tree that still carries
    // its sentinel and would pass for a complete install.
    yield* fs.remove(target, { recursive: true, force: true }).pipe(
      Effect.andThen(fs.rename(versionDir, target)),
      Effect.andThen(Effect.sync(() => pruned.push(target))),
      Effect.catch((cause) =>
        Effect.logWarning("Could not remove an unused t3 runtime.", { version, cause }),
      ),
    );
  }
  return pruned;
});

/**
 * Every update publishes a new immutable runtime, so without pruning they pile up. Removes
 * completed runtimes nothing can start again and keeps: the caller's versions, everything the
 * service state references (active, rollback, pending), the `t3` launchers' targets, runtimes
 * named in `references` (such as the service unit) or in a running process's command line, and
 * anything newer than the caller's versions. Staging directories, symlinks, and unexpected entries
 * are never touched. Returns the versions removed.
 */
export const prunePinnedRuntimes = Effect.fn("cloud.pinned_runtime.prune")(function* (input: {
  readonly baseDir: string;
  readonly keep: ReadonlyArray<string>;
  readonly references?: ReadonlyArray<string>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // Only selection and the renames race an install; deleting renamed trees can take a while.
  const pruned = yield* pinnedRuntimeInstallLock.withPermit(
    selectUnusedRuntimes({ ...input, references: input.references ?? [] }),
  );
  const removed: Array<string> = [];
  for (const target of pruned) {
    yield* fs.remove(target, { recursive: true, force: true }).pipe(
      Effect.andThen(
        Effect.sync(() =>
          removed.push(path.basename(target).slice(PINNED_RUNTIME_PRUNED_PREFIX.length)),
        ),
      ),
      Effect.catch((cause) =>
        Effect.logWarning("Could not remove an unused t3 runtime.", { target, cause }),
      ),
    );
  }
  return removed;
});
