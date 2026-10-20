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

/** Versions the `t3` shims written by the install scripts and `t3 update` point at. */
const shellLauncherVersions = Effect.fn("cloud.pinned_runtime.shell_launcher_versions")(function* (
  versionsDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const environment = yield* HostProcess.Environment;
  const home = yield* HostProcess.HomeDirectory;
  const directories = [
    environment["T3CODE_INSTALL_BIN_DIR"] || path.join(home, ".local", "bin"),
    ...(environment["PATH"] ?? environment["Path"] ?? "").split(platform === "win32" ? ";" : ":"),
  ].filter((directory) => directory.trim().length > 0);
  const versions = new Set<string>();
  for (const directory of directories) {
    const target =
      platform === "win32"
        ? yield* fs.readFileString(path.join(directory, "t3.cmd")).pipe(
            Effect.map((contents) => /^"([^"]+)"/m.exec(contents)?.[1]),
            Effect.orElseSucceed(() => undefined),
          )
        : yield* fs.readLink(path.join(directory, "t3")).pipe(
            Effect.map((link) => path.resolve(directory, link)),
            Effect.orElseSucceed(() => undefined),
          );
    if (target === undefined) continue;
    const relative = path.relative(versionsDir, path.resolve(target));
    if (relative.startsWith("..") || path.isAbsolute(relative)) continue;
    const version = relative.split(path.sep)[0];
    if (version) versions.add(version);
  }
  return versions;
});

const runningCommandLines = Effect.fn("cloud.pinned_runtime.running_command_lines")(function* () {
  const runner = yield* ProcessRunner.ProcessRunner;
  const platform = yield* HostProcess.Platform;
  const result = yield* runner.run({
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
  });
  return result.code === 0 && result.stdout.length > 0 ? Option.some(result.stdout) : Option.none();
});

/**
 * Every update publishes a new immutable runtime, so without pruning they pile up. Removes
 * completed runtimes nothing can start again and keeps: the caller's versions, everything the
 * service state references (active, rollback, pending), the `t3` shell launcher's target, any
 * runtime a running process uses (an older service launcher keeps serving newer children), and
 * anything newer than the caller's versions, which another process may be installing. Staging
 * directories, symlinks, and unexpected entries are never touched.
 */
export const prunePinnedRuntimes = Effect.fn("cloud.pinned_runtime.prune")(function* (input: {
  readonly baseDir: string;
  readonly keep: ReadonlyArray<string>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const versionsDir = pinnedRuntimeVersionsDir(path, input.baseDir);
  const entries = yield* fs.readDirectory(versionsDir).pipe(Effect.orElseSucceed(() => []));

  // An interrupted prune has already renamed its runtime out of the way.
  for (const entry of entries) {
    if (entry.startsWith(PINNED_RUNTIME_PRUNED_PREFIX)) {
      yield* fs.remove(path.join(versionsDir, entry), { recursive: true, force: true });
    }
  }

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
  if (completed.length === 0) return [];

  const keep = new Set(input.keep);
  const stateText = yield* fs
    .readFileString(path.join(input.baseDir, "runtime", SERVICE_STATE_FILE))
    .pipe(Effect.option);
  if (Option.isSome(stateText)) {
    const state = parseServiceState(stateText.value);
    // State this version cannot read may reference runtimes in ways it does not know.
    if (state === undefined) return [];
    keep.add(state.activeVersion);
    if (state.update !== undefined) {
      keep.add(state.update.fromVersion);
      keep.add(state.update.targetVersion);
    }
  }
  for (const version of yield* shellLauncherVersions(versionsDir)) keep.add(version);
  const commandLines = yield* runningCommandLines();
  // Without a process list, a runtime in use would look unused.
  if (Option.isNone(commandLines)) return [];

  const removable = completed.filter(
    (version) =>
      !keep.has(version) &&
      input.keep.every((kept) => compareExactServiceVersions(version, kept) < 0) &&
      !commandLines.value.includes(path.join(versionsDir, version) + path.sep),
  );
  const removed: Array<string> = [];
  for (const version of removable) {
    const pruned = path.join(versionsDir, `${PINNED_RUNTIME_PRUNED_PREFIX}${version}`);
    // Renaming first means an interrupted removal never leaves a partial tree that still carries
    // its sentinel and would pass for a complete install.
    yield* fs.rename(path.join(versionsDir, version), pruned).pipe(
      Effect.andThen(fs.remove(pruned, { recursive: true, force: true })),
      Effect.andThen(Effect.sync(() => removed.push(version))),
      Effect.catch((cause) =>
        Effect.logWarning("Could not remove an unused t3 runtime.", { version, cause }),
      ),
    );
  }
  return removed;
}, pinnedRuntimeInstallLock.withPermit);
