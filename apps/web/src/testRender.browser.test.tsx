import { expect, it } from "vite-plus/test";

import { render } from "../test/render";

function Crash(): never {
  throw new Error("crashed while rendering");
}

it("fails the render when the component throws inside the test router", async () => {
  await expect(render(<Crash />)).rejects.toThrow("crashed while rendering");
});
