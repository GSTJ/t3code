import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  RouterProvider,
  type AnyRouter,
} from "@tanstack/react-router";
import { act as reactAct, createContext, use, type ReactNode } from "react";
import { cleanup, render as renderComponent, renderHook } from "vitest-browser-react";

export { cleanup, renderHook };
export type { RenderResult } from "vitest-browser-react";

declare global {
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined;
}

// React warns about every update outside act() while this flag is on, and a real
// browser drives updates from userEvent, timers and network that act() cannot wrap.
// Turn it on only for the duration of act(), as vitest-browser-react's render does.
export async function act<T>(callback: () => T | Promise<T>): Promise<T> {
  const previous = globalThis.IS_REACT_ACT_ENVIRONMENT;
  globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  try {
    return await reactAct(async () => await callback());
  } finally {
    globalThis.IS_REACT_ACT_ENVIRONMENT = previous;
  }
}

const RoutedUi = createContext<ReactNode>(null);

function RoutedUiOutlet() {
  return use(RoutedUi);
}

// Links and navigation hooks need a router even when the test is not about routing.
function createTestRouter(path: string): AnyRouter {
  return createRouter({
    routeTree: createRootRoute({ component: RoutedUiOutlet }),
    history: createMemoryHistory({ initialEntries: [path] }),
    // The router's error UI would otherwise turn a crash into a passing negative assertion.
    disableGlobalCatchBoundary: true,
  });
}

export interface RenderOptions {
  /**
   * Rendering into the same container again re-renders in place, like `root.render`,
   * keeping its first router. Use a new container after `cleanup()`.
   */
  readonly container?: HTMLElement;
  /** Initial location of the test router, or `false` to render without one. */
  readonly router?: string | false;
}

type Wrapper = (props: { children: ReactNode }) => ReactNode;
const wrappers = new WeakMap<HTMLElement, Wrapper>();

function routerWrapper(path: string): Wrapper {
  const router = createTestRouter(path);
  return ({ children }) => (
    <RoutedUi value={children}>
      <RouterProvider router={router} />
    </RoutedUi>
  );
}

export function render(ui: ReactNode, { container, router = "/" }: RenderOptions = {}) {
  const target = container ? { container } : {};
  if (router === false) return renderComponent(ui, target);
  // A new wrapper component on re-render would remount the tree under test.
  let wrapper = container && wrappers.get(container);
  if (!wrapper) {
    wrapper = routerWrapper(router);
    if (container) wrappers.set(container, wrapper);
  }
  return renderComponent(ui, { ...target, wrapper });
}
