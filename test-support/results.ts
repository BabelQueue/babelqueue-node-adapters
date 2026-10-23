// Shared results.json collector for the adapter packages' conformance runners (ADR-0032).
//
// Each package that carries a conformance binding (redis, kafka, sqs) reports every case it runs
// through `ConformanceResults.run`, and writes a results document in the `schema/results.example.json`
// shape when `BQ_CONFORMANCE_RESULTS` is set. The variable names a file; because several packages
// share one repository, the package slug is inserted before the extension
// (`results.json` -> `results.redis.json`) so the runs do not overwrite each other.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";

export interface CaseResult {
  status: "pass" | "fail" | "skip";
  reason?: string;
}

export class ConformanceResults {
  private readonly sections: Record<string, Record<string, CaseResult>> = {};

  /**
   * @param packageRoot absolute directory of the package (holds package.json and test/conformance)
   * @param slug        short package name used in the file name, e.g. "redis"
   * @param bindings    the manifest scopes the package carries, e.g. ["redis"]
   */
  constructor(
    private readonly packageRoot: string,
    private readonly slug: string,
    private readonly bindings: string[],
  ) {}

  record(section: string, id: string, result: CaseResult): void {
    (this.sections[section] ??= {})[id] = result;
  }

  /** Run one conformance case; its outcome lands in the document whether it passes or fails. */
  async run(section: string, id: string, fn: () => void | Promise<void>): Promise<void> {
    try {
      await fn();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.record(section, id, { status: "fail", reason: message.slice(0, 500) || "failed" });
      throw error;
    }
    this.record(section, id, { status: "pass" });
  }

  /** Cases keyed by their `name`, or `<section>#<index>` when the case has none. */
  static caseId(section: string, testCase: { name?: unknown }, index: number): string {
    return typeof testCase.name === "string" ? testCase.name : `${section}#${index}`;
  }

  /** Write the document if `BQ_CONFORMANCE_RESULTS` is set; returns the path written, if any. */
  write(): string | undefined {
    const target = process.env["BQ_CONFORMANCE_RESULTS"];
    if (target === undefined || target === "") return undefined;
    const pkg = JSON.parse(readFileSync(join(this.packageRoot, "package.json"), "utf8")) as { name: string; version: string };
    const version = readFileSync(join(this.packageRoot, "test", "conformance", "CONFORMANCE_VERSION"), "utf8").trim();
    const document = {
      sdk: pkg.name,
      version: pkg.version,
      conformance_version: version,
      generated_at: new Date().toISOString(),
      bindings: this.bindings,
      sections: this.sections,
    };
    const file = basename(target).replace(/(\.json)?$/, `.${this.slug}.json`);
    const path = join(dirname(target), file);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, `${JSON.stringify(document, null, 2)}\n`);
    return path;
  }
}
