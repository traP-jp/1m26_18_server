import { defineConfig } from "orval"

export default defineConfig({
  om26_18: {
    input: "./api/openapi.json",
    hooks: {
      afterAllFilesWrite: "deno fmt",
    },
    output: {
      mode: "tags-split",
      target: "./api/om26_18.ts",
    },
  },
})
