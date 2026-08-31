#!/usr/bin/env node

import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { createInterface } from "readline/promises";
import { stdin as input, stdout as output } from "process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const packageJsonPath = path.join(repoRoot, "package.json");

function run(command, args, options = {}) {
  const capture = options.capture === true;
  const allowFailure = options.allowFailure === true;

  const result = spawnSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: capture ? ["inherit", "pipe", "pipe"] : "inherit"
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0 && !allowFailure) {
    const rendered = [command, ...args].join(" ");
    throw new Error(`Command failed (${result.status}): ${rendered}`);
  }

  return {
    status: result.status ?? 1,
    stdout: capture ? (result.stdout || "").trim() : "",
    stderr: capture ? (result.stderr || "").trim() : ""
  };
}

function parseVersionInfo() {
  const pkg = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
  const packageVersion = pkg.version;

  if (!packageVersion) {
    throw new Error("Could not read package.json version.");
  }

  return packageVersion;
}

async function askReleaseMode(rl) {
  console.log("");
  console.log("Release mode:");
  console.log("1. Use what's already pushed to GitHub (release from origin/main as-is)");
  console.log("2. Git add/commit/push everything now, then release from main");
  console.log("");

  while (true) {
    const answer = (await rl.question("Choose 1 or 2: ")).trim().toLowerCase();

    if (answer === "1" || answer === "2") {
      return answer;
    }

    console.log("Please answer 1 or 2.");
  }
}

async function commitAndPushMain(rl) {
  const currentBranch = run("git", ["branch", "--show-current"], { capture: true }).stdout;
  if (currentBranch !== "main") {
    throw new Error(
      `Current branch is "${currentBranch}". Switch to main before using option 2, since releases are created from main.`
    );
  }

  const status = run("git", ["status", "--porcelain"], { capture: true }).stdout;
  if (status.length > 0) {
    const commitMessage = (await rl.question("Commit message: ")).trim();
    if (!commitMessage) {
      throw new Error("Commit message cannot be empty.");
    }

    run("git", ["add", "-A"]);
    run("git", ["commit", "-m", commitMessage]);
  } else {
    console.log("No local file changes to commit.");
  }

  run("git", ["push", "origin", "main"]);
}

function ensureToolsAvailable() {
  run("git", ["--version"], { capture: true });
  run("gh", ["--version"], { capture: true });
}

function createGithubRelease(version) {
  const tag = `v${version}`;
  const exists = run("gh", ["release", "view", tag], { allowFailure: true, capture: true });

  if (exists.status === 0) {
    throw new Error(`GitHub release ${tag} already exists.`);
  }

  run("gh", ["release", "create", tag, "--target", "main", "--title", tag, "--generate-notes"]);
  console.log(`Created GitHub release ${tag} from main.`);
}

async function main() {
  ensureToolsAvailable();
  const version = parseVersionInfo();
  console.log(`Version check OK: ${version}`);

  const rl = createInterface({ input, output });
  try {
    const mode = await askReleaseMode(rl);

    if (mode === "2") {
      await commitAndPushMain(rl);
    } else {
      console.log("Using already-pushed main branch state.");
    }

    createGithubRelease(version);
  } finally {
    rl.close();
  }
}

main().catch((error) => {
  console.error("");
  console.error(`Release failed: ${error.message}`);
  process.exit(1);
});
