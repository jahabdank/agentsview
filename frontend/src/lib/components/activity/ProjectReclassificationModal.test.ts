// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { fireEvent, screen } from "@testing-library/svelte";
import { mount, tick, unmount } from "svelte";
import ProjectReclassificationModal from "./ProjectReclassificationModal.svelte";

const api = vi.hoisted(() => ({
  candidates: vi.fn(),
  preview: vi.fn(),
  apply: vi.fn(),
}));

vi.mock("../../api/generated/index", () => ({
  ActivityService: {
    getApiV1ActivityProjectReclassificationCandidates: api.candidates,
  },
  SettingsService: {
    postApiV1SettingsWorktreeMappingsPreview: api.preview,
    postApiV1SettingsWorktreeMappingsReclassify: api.apply,
  },
}));
vi.mock("../../api/runtime.js", () => ({
  callGenerated: vi.fn((request: () => Promise<unknown>) => request()),
  isAbortError: vi.fn(() => false),
}));

const candidate = {
  id: "candidate-1",
  machine: "remote.example",
  suggested_prefix: "/srv/worktrees/example/repo/branch",
  contributing_sessions: 3,
  distinct_cwds: 2,
  evidence_kind: "identity",
  evidence_root: "/srv/worktrees/example/repo/branch",
  examples: [],
  available: true,
};

const preview = {
  mapping_token: "token-1",
  normalized_project: "target-project",
  matched_sessions: 7,
  updated_sessions: 6,
  distinct_projects: 2,
  project_samples: [
    { project: "wrong-project", count: 6 },
    { project: "another-project", count: 1 },
  ],
  session_samples: [],
};

async function flush() {
  await tick();
  await Promise.resolve();
  await tick();
}

describe("ProjectReclassificationModal", () => {
  let component: ReturnType<typeof mount> | undefined;

  beforeEach(() => {
    vi.useFakeTimers();
    api.candidates.mockReset();
    api.preview.mockReset();
    api.apply.mockReset();
    api.candidates.mockResolvedValue({ candidates: [candidate] });
    api.preview.mockResolvedValue(preview);
    api.apply.mockResolvedValue({ mapping: {}, result: preview });
  });

  afterEach(() => {
    if (component) unmount(component);
    component = undefined;
    document.body.innerHTML = "";
    vi.useRealTimers();
  });

  function render(overrides: Record<string, unknown> = {}) {
    component = mount(ProjectReclassificationModal, {
      target: document.body,
      props: {
        projectLabel: "wrong-project",
        projectKey: "pl1:sha256:wrong",
        projects: [{ name: "target-project", session_count: 12 }],
        queryParams: {
          preset: "custom",
          from: "2026-07-01T00:00:00Z",
          to: "2026-07-02T00:00:00Z",
          machine: "remote.example",
          automation: "all",
        },
        onclose: vi.fn(),
        onRefresh: vi.fn().mockResolvedValue(true),
        onComplete: vi.fn(),
        ...overrides,
      },
    });
  }

  async function chooseTarget() {
    await fireEvent.click(screen.getByTitle("Target project"));
    await fireEvent.mouseDown(screen.getByRole("option", { name: "target-project (12)" }));
    await vi.advanceTimersByTimeAsync(300);
    await flush();
  }

  it("uses the clicked scope, preselects one candidate, and previews full-archive impact", async () => {
    render();
    await flush();

    expect(api.candidates).toHaveBeenCalledWith({
      preset: "custom",
      from: "2026-07-01T00:00:00Z",
      to: "2026-07-02T00:00:00Z",
      machine: "remote.example",
      automation: "all",
      clickedProject: "wrong-project",
      clickedProjectKey: "pl1:sha256:wrong",
    });
    expect(screen.getByText(/Originally shown as wrong-project/)).toBeTruthy();
    expect(screen.getByDisplayValue(candidate.suggested_prefix)).toBeTruthy();
    expect(screen.getByText("remote.example")).toBeTruthy();

    await chooseTarget();

    expect(api.preview).toHaveBeenCalledWith({
      requestBody: {
        machine: "remote.example",
        path_prefix: candidate.suggested_prefix,
        project: "target-project",
        original_project: "wrong-project",
        layout: "explicit",
        enabled: true,
      },
    });
    expect(screen.getByText(/7 sessions matched/)).toBeTruthy();
    expect(screen.getByText(/6 sessions will change/)).toBeTruthy();
    expect(screen.getByText(/2 projects/)).toBeTruthy();
    expect(screen.getByRole("alert").textContent).toContain("multiple projects");
  });

  it("debounces prefix edits and never accepts an obsolete preview token", async () => {
    render();
    await flush();
    await fireEvent.click(screen.getByTitle("Target project"));
    await fireEvent.mouseDown(screen.getByRole("option", { name: "target-project (12)" }));
    const prefix = screen.getByLabelText("Path prefix");
    await fireEvent.input(prefix, { target: { value: "/srv/first" } });
    await vi.advanceTimersByTimeAsync(200);
    await fireEvent.input(prefix, { target: { value: "/srv/final" } });
    await vi.advanceTimersByTimeAsync(300);
    await flush();

    expect(api.preview).toHaveBeenCalledTimes(1);
    expect(api.preview.mock.calls[0]![0].requestBody.path_prefix).toBe("/srv/final");
  });

  it("commits a custom target and blocks a zero-match preview", async () => {
    api.preview.mockResolvedValueOnce({
      ...preview,
      mapping_token: "empty-token",
      matched_sessions: 0,
      updated_sessions: 0,
      distinct_projects: 0,
      project_samples: [],
    });
    render();
    await flush();
    await fireEvent.click(screen.getByTitle("Target project"));
    await fireEvent.input(screen.getByRole("combobox"), {
      target: { value: "new-project" },
    });
    await fireEvent.mouseDown(screen.getByRole("option", { name: 'Use project "new-project"' }));
    await vi.advanceTimersByTimeAsync(300);
    await flush();

    expect(api.preview.mock.calls[0]![0].requestBody.project).toBe("new-project");
    expect(screen.getByText(/matches no sessions/)).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Apply reclassification" }) as HTMLButtonElement,
    ).toHaveProperty("disabled", true);
  });

  it("discards a preview response superseded by a later draft", async () => {
    let resolveFirst!: (value: typeof preview) => void;
    api.preview
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirst = resolve;
          }),
      )
      .mockResolvedValueOnce({ ...preview, mapping_token: "latest-token" });
    render();
    await flush();
    await chooseTarget();
    const prefix = screen.getByLabelText("Path prefix");
    await fireEvent.input(prefix, { target: { value: "/srv/latest" } });
    await vi.advanceTimersByTimeAsync(300);
    await flush();
    resolveFirst({ ...preview, mapping_token: "obsolete-token" });
    await flush();

    await fireEvent.click(screen.getByRole("button", { name: "Apply reclassification" }));
    await flush();
    expect(api.apply.mock.calls[0]![0].requestBody.mapping_token).toBe("latest-token");
  });

  it("requires an explicit choice for multiple candidates and explains unavailable cwd evidence", async () => {
    api.candidates.mockResolvedValue({
      candidates: [
        candidate,
        {
          ...candidate,
          id: "candidate-2",
          machine: "other.example",
          suggested_prefix: "",
          available: false,
        },
      ],
    });
    render();
    await flush();

    expect(screen.getAllByText("Choose a worktree")).toHaveLength(2);
    expect(screen.queryByLabelText("Path prefix")).toBeNull();
    await fireEvent.click(screen.getByTitle("Choose a worktree"));
    await fireEvent.mouseDown(screen.getByRole("option", { name: /other\.example/ }));
    await flush();
    expect(screen.getByText(/working directory is unavailable/)).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Apply reclassification" }) as HTMLButtonElement,
    ).toHaveProperty("disabled", true);
  });

  it("applies exactly once and offers only refresh retry after a post-commit failure", async () => {
    const onRefresh = vi.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true);
    const onComplete = vi.fn();
    render({ onRefresh, onComplete });
    await flush();
    await chooseTarget();

    const apply = screen.getByRole("button", { name: "Apply reclassification" });
    await fireEvent.click(apply);
    await flush();

    expect(api.apply).toHaveBeenCalledTimes(1);
    expect(api.apply.mock.calls[0]![0].requestBody.mapping_token).toBe("token-1");
    expect(screen.queryByRole("button", { name: "Apply reclassification" })).toBeNull();
    expect(screen.getByText(/Applied, but Activity could not refresh/)).toBeTruthy();
    await fireEvent.click(screen.getByRole("button", { name: "Retry refresh" }));
    await flush();
    expect(api.apply).toHaveBeenCalledTimes(1);
    expect(onRefresh).toHaveBeenCalledTimes(2);
    expect(onComplete).toHaveBeenCalledTimes(1);
  });

  it("refreshes the preview after a mapping-set conflict", async () => {
    api.apply.mockRejectedValueOnce(Object.assign(new Error("changed"), { status: 409 }));
    render();
    await flush();
    await chooseTarget();
    await fireEvent.click(screen.getByRole("button", { name: "Apply reclassification" }));
    await flush();

    expect(api.apply).toHaveBeenCalledTimes(1);
    expect(api.preview).toHaveBeenCalledTimes(2);
    expect(screen.getByText(/changed since the preview/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Apply reclassification" })).toBeTruthy();
  });
});
