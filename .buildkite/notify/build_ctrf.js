#!/usr/bin/env node
// Merge per-scenario reports/run.json into a single CTRF JSON for slack-ctrf.
//
// Reads (under <art_root>):
//   <step>/**/run.json      — sentinel per-scenario summary
//                             (schema: sentinel/plugins/scenario_summary.py::_write_run_json)
//   <step>/report_ids.txt   — "<artifact_uuid> <job_uuid>" for reports/report.html;
//                             assembled into the direct inline download URL so the
//                             Slack "report" link opens the rendered HTML.
//   <step>/job_id.txt       — Buildkite job UUID (from run.json). Fallback used for
//                             the report link when report.html is missing — points
//                             to the step page with ?job=<uuid>.
// Only step directories that actually produced run.json are reported, so
// scenarios that didn't run in this build don't clutter the Slack summary.
// Writes CTRF JSON to stdout. Consumed by slack-ctrf custom.

'use strict';

const fs = require('fs');
const path = require('path');

const artRoot = process.argv[2];
if (!artRoot) {
  console.error('usage: build_ctrf.js <art_root>');
  process.exit(2);
}

const buildUrl = process.env.BUILDKITE_BUILD_URL || '';
const buildNumber = process.env.BUILDKITE_BUILD_NUMBER || '';
const branch = process.env.BUILDKITE_BRANCH || '';
const commit = (process.env.BUILDKITE_COMMIT || '').slice(0, 8);
// Epoch seconds stamped by the first pipeline step via
// `buildkite-agent meta-data set build:started_at`. Buildkite doesn't
// expose the build start time as a native env var, so notify passes it
// through after reading the meta-data.
const buildStartedAt = process.env.BUILD_STARTED_AT || '';

function findFile(dir, name) {
  if (!fs.existsSync(dir)) return null;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      const found = findFile(full, name);
      if (found) return found;
    } else if (entry.name === name) {
      return full;
    }
  }
  return null;
}

function readOpt(stepDir, name) {
  const p = path.join(stepDir, name);
  return fs.existsSync(p) ? fs.readFileSync(p, 'utf8').trim() : '';
}

// Build the direct inline-download URL for a scenario's report.html.
// Transform: https://host/orgs/... -> https://host/api/orgs/...
// Suffix:    /jobs/<jobId>/artifacts/<artifactId>/download?inline=1
function buildReportUrl(buildUrl, jobId, artifactId) {
  if (!buildUrl || !jobId || !artifactId) return '';
  const api = buildUrl.replace('/orgs/', '/api/orgs/');
  return `${api}/jobs/${jobId}/artifacts/${artifactId}/download?inline=1`;
}

function formatDuration(totalSec) {
  const s = Math.max(0, Math.round(totalSec));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return rem === 0 ? `${m}m` : `${m}m ${rem}s`;
}

const stepKeys = fs.existsSync(artRoot)
  ? fs.readdirSync(artRoot, { withFileTypes: true })
      .filter((e) => e.isDirectory())
      .map((e) => e.name)
      .sort()
  : [];

const scenarios = [];
const totals = { passed: 0, failed: 0, skipped: 0 };

for (const stepKey of stepKeys) {
  const stepDir = path.join(artRoot, stepKey);
  const runJsonPath = findFile(stepDir, 'run.json');
  if (!runJsonPath) continue;

  const [artifactId = '', reportJobId = ''] = readOpt(stepDir, 'report_ids.txt').split(/\s+/);
  const fallbackJobId = readOpt(stepDir, 'job_id.txt');
  const reportUrl = buildReportUrl(buildUrl, reportJobId, artifactId)
    || (fallbackJobId && buildUrl ? `${buildUrl}?job=${fallbackJobId}` : buildUrl);

  const data = JSON.parse(fs.readFileSync(runJsonPath, 'utf8'));
  const entries = Object.entries(data.scenarios || {});
  if (entries.length === 0) continue;

  for (const [, stats] of entries) {
    const passed = stats.passed || 0;
    const failed = stats.failed || 0;
    const skipped = stats.skipped || 0;

    scenarios.push({
      key: stepKey,
      name: stats.name || stepKey,
      passed,
      failed,
      skipped,
      status: failed > 0 ? 'failed' : 'passed',
      status_emoji: failed > 0 ? ':x:' : ':white_check_mark:',
      report_url: reportUrl,
    });

    totals.passed += passed;
    totals.failed += failed;
    totals.skipped += skipped;
  }
}

// Build wall-clock duration: from the timestamp the first step stamped
// (build-image's first action) to the moment notify runs. notify depends
// on all scenarios, so this approximates Buildkite's "Started -> Finished"
// (excludes pre-first-step agent scheduling and the parallel cleanup step
// that runs alongside notify). Fall back to 0 if the meta-data is missing
// (upstream step never ran, or local testing).
const startedAtSec = buildStartedAt ? Number(buildStartedAt) : NaN;
const elapsedSec = Number.isFinite(startedAtSec)
  ? Math.max(0, Date.now() / 1000 - startedAtSec)
  : 0;

// No scenarios -> upstream step failed before tests ran (e.g. build-image).
// Treat as an overall failure so the header doesn't falsely show a green check.
const overallFailed = totals.failed > 0 || scenarios.length === 0;

const ctrf = {
  results: {
    tool: { name: 'pytest-sentinel' },
    summary: {
      tests: totals.passed + totals.failed + totals.skipped,
      passed: totals.passed,
      failed: totals.failed,
      skipped: totals.skipped,
      pending: 0,
      other: 0,
      start: 0,
      stop: 0,
    },
    tests: [],
    environment: {
      buildName: buildNumber,
      buildUrl,
      branchName: branch,
      commit,
      extra: {
        scenarios,
        duration: formatDuration(elapsedSec),
        overall_status: overallFailed ? 'failed' : 'passed',
        overall_emoji: overallFailed ? ':x:' : ':white_check_mark:',
      },
    },
  },
};

process.stdout.write(JSON.stringify(ctrf, null, 2));
