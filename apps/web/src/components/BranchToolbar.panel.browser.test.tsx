import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";
import { act, render as renderUi } from "../../test/render";

const state = vi.hoisted(() => ({
  worktreePath: "/tmp/worktree" as string | null,
  showContextMenu: vi.fn(),
  writeTextToClipboard: vi.fn(),
}));

vi.mock(import("../localApi"), async (original) => ({
  ...(await original()),
  readLocalApi: (() => ({ contextMenu: { show: state.showContextMenu } })) as never,
}));
vi.mock(import("../hooks/useCopyToClipboard"), async (original) => ({
  ...(await original()),
  writeTextToClipboard: state.writeTextToClipboard,
  useCopyToClipboard: ((options: { target: string }) => ({
    copyToClipboard: (command: string) => state.writeTextToClipboard(command, options.target),
    isCopied: false,
  })) as never,
}));
vi.mock(import("./BranchToolbarBranchSelector"), async (original) => ({
  ...(await original()),
  BranchToolbarBranchSelector: (() => null) as never,
}));
vi.mock(import("../state/entities"), async (original) => ({
  ...(await original()),
  useProject: (() => ({ workspaceRoot: "/tmp/project" })) as never,
  useThreadShellsForProjectRefs: () => [],
}));

import { BranchToolbar } from "./BranchToolbar";
import { ComposerSurface } from "./chat/ComposerSurface";
import type { ServerUpdateTarget } from "./ServerUpdateAction";

function render(
  props: Partial<ComponentProps<typeof BranchToolbar>>,
  {
    container,
    worktreePath = "/tmp/worktree",
  }: { container?: HTMLElement; worktreePath?: string | null } = {},
) {
  state.worktreePath = worktreePath;
  state.showContextMenu.mockReset().mockResolvedValue("copy-path");
  state.writeTextToClipboard.mockReset().mockResolvedValue(true);
  return renderUi(
    <ComposerSurface.Shell>
      <BranchToolbar
        layout="panel"
        panelSection="workspace"
        environmentId={EnvironmentId.make("local")}
        threadId={ThreadId.make("thread")}
        projectId={ProjectId.make("project")}
        worktreePath={state.worktreePath}
        isServerThread
        showGitControls
        envMode="local"
        envLocked={false}
        startFromOrigin={false}
        onStartFromOriginChange={vi.fn()}
        onEnvModeChange={vi.fn()}
        {...props}
      />
    </ComposerSurface.Shell>,
    container ? { container } : {},
  );
}

it("keeps machine choices usable when the combined row's workspace is locked", async () => {
  const onEnvironmentChange = vi.fn();
  await using screen = await render({
    onEnvironmentChange,
    availableEnvironments: ["local", "remote"].map((id) => ({
      environmentId: EnvironmentId.make(id),
      projectId: ProjectId.make("project"),
      label: id,
      isPrimary: id === "local",
      machine: "server",
    })),
  });
  const trigger = screen.container.querySelector("button")!;
  expect(trigger.textContent).toBe("local");
  expect(screen.container.querySelectorAll("button")).toHaveLength(1);

  // A ctrl-click is a macOS secondary click; it must not open the menu.
  await act(async () => {
    trigger.dispatchEvent(new MouseEvent("mousedown", { bubbles: true, ctrlKey: true }));
  });
  expect(document.querySelector('[role="menu"]')).toBeNull();

  await userEvent.click(trigger);
  await expect
    .element(page.getByRole("menuitemradio", { name: "New worktree" }))
    .toHaveAttribute("aria-disabled", "true");
  await userEvent.click(page.getByRole("menuitemradio", { name: "remote" }));
  expect(onEnvironmentChange).toHaveBeenCalledWith("remote");
});

it("shows a started thread's primary machine as a static label in the composer", async () => {
  await using screen = await render({
    layout: "composer",
    showGitControls: false,
    availableEnvironments: ["local", "remote"].map((id) => ({
      environmentId: EnvironmentId.make(id),
      projectId: ProjectId.make("project"),
      label: id,
      isPrimary: id === "local",
      machine: "server",
    })),
  });
  expect(screen.container.textContent).toBe("local");
  expect(screen.container.querySelector("button")).toBeNull();
});

it.each([
  ["local", null, "/tmp/project"],
  ["worktree", null, null],
  ["worktree", "/tmp/worktree", "/tmp/worktree"],
  ["worktree", "C:/repo/worktree", String.raw`C:\repo\worktree`],
] as const)(
  "copies only an existing workspace path with mode %s and worktree %s",
  async (envMode, worktreePath, copiedPath) => {
    await using screen = await render({ envMode }, { worktreePath });
    const event = new MouseEvent("contextmenu", { bubbles: true, cancelable: true });
    await act(async () => {
      screen.container.querySelector('[aria-label="Run context"]')!.dispatchEvent(event);
    });
    expect(event.defaultPrevented).toBe(copiedPath !== null);
    if (copiedPath === null) {
      expect(state.showContextMenu).not.toHaveBeenCalled();
      expect(state.writeTextToClipboard).not.toHaveBeenCalled();
    } else {
      await vi.waitFor(() =>
        expect(state.writeTextToClipboard).toHaveBeenCalledWith(copiedPath, "workspace path"),
      );
    }
  },
);

it.each([
  { layout: "wide Git", width: 900, showGitControls: true },
  { layout: "narrow Git", width: 360, showGitControls: true },
  { layout: "sole primary non-Git", width: 360, showGitControls: false },
])(
  "copies the pending update from the $layout composer chip",
  async ({ width, showGitControls }) => {
    await page.viewport(1000, 700);
    const container = document.createElement("div");
    container.style.width = `${width}px`;
    container.style.setProperty("--chat-content-max-width", `${width}px`);
    const serverUpdate = {
      environmentId: EnvironmentId.make("local"),
      serverLabel: "Build box",
      serverVersion: "0.0.30",
      targetVersion: "0.0.31",
      selfUpdate: null,
      installation: { kind: "npm-global", prefix: "/opt/node" },
    } satisfies ServerUpdateTarget & { serverVersion: string };
    await using screen = await render(
      {
        layout: "composer",
        envLocked: showGitControls,
        showGitControls,
        availableEnvironments: [
          {
            environmentId: serverUpdate.environmentId,
            projectId: ProjectId.make("project"),
            label: serverUpdate.serverLabel,
            isPrimary: true,
            machine: "server",
          },
        ],
        serverUpdate,
      },
      { container },
    );

    expect(
      screen.container.querySelector('[data-slot="composer-shell"]')!.getBoundingClientRect().width,
    ).toBe(width);
    const chip = page.getByRole("button", { name: /Build box|Copy update command/ });
    await expect.element(chip).toBeVisible();
    await userEvent.hover(chip);
    await expect.element(page.getByRole("tooltip")).toMatchTextContent("0.0.30");
    await expect.element(page.getByRole("tooltip")).toMatchTextContent("0.0.31");
    await userEvent.click(chip);
    await vi.waitFor(() =>
      expect(state.writeTextToClipboard).toHaveBeenCalledWith(
        "npm install --global --prefix '/opt/node' t3@0.0.31",
        "update command",
      ),
    );
  },
);
