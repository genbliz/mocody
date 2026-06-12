import js from "@eslint/js";
import { defineConfig, globalIgnores } from "eslint/config";
import tseslint from "typescript-eslint";
import pluginNode from "eslint-plugin-n";
import pluginPrettier from "eslint-plugin-prettier";
import configPrettier from "eslint-config-prettier";

export default defineConfig([
  // 1. Corrected global ignores syntax
  globalIgnores([
    //
    "**/node_modules/**",
    "**/dist/**",
    "**/build/**",
    "**/dist-func/**",
    "**/cdk.out/**",
    "**/.logs/**",
  ]),

  // 2. Main configuration block
  {
    files: ["src/**/*.{ts,js,cjs,cts,mts,mjs,tsx,jsx}"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      // Modern TS Parser to read your typed code safely
      parser: tseslint.parser,
      globals: {
        // FIX: Prefixed with "flat/" to access properties safely
        ...(pluginNode?.configs?.["flat/recommended-module"]?.languageOptions?.globals || {}),
      },
    },
    plugins: {
      n: pluginNode,
      "@typescript-eslint": tseslint.plugin, // Provides TS matching rules
      prettier: pluginPrettier,
    },
    rules: {
      // Standard JavaScript recommended rules
      ...js.configs.recommended.rules,

      // TypeScript native recommended rules
      ...tseslint?.configs?.recommended?.rules,

      // FIX: Prefixed with "flat/" to access rule sets safely
      ...pluginNode.configs?.["flat/recommended-module"]?.rules,

      // Prettier integration rules
      ...pluginPrettier?.configs?.recommended?.rules,
      "prettier/prettier": 0,
      "no-redeclare": "off",
      "no-unused-vars": "off",
      "no-console": 0,
      "no-undef": 0,
      //
      eqeqeq: 0,
      "prefer-const": "error",
      "no-useless-escape": 0,
      "no-useless-assignment": 0,
      "@typescript-eslint/consistent-type-imports": 0,
      // Turn off conflicting Node rules if using ES Modules
      "n/no-missing-import": "off",
      "n/no-process-exit": "off",
      "n/no-unsupported-features/node-builtins": 0,
      //
      "@typescript-eslint/no-empty-object-type": 0,
      "@typescript-eslint/no-redeclare": "off",
      "@typescript-eslint/no-unused-vars": "off",
      "n/hashbang": "off",
      "@typescript-eslint/consistent-type-imports": [
        "error",
        {
          prefer: "type-imports",
          fixStyle: "separate-type-imports",
        },
      ],
    },
  },

  // 3. Disable rules that conflict with Prettier format
  configPrettier,
]);
