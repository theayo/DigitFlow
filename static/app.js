"use strict";

// Two screens over the internal API. No build step and no framework: the whole
// job is polling one endpoint and rendering two tables.
//
// Timestamps that end in `_nsk` already come from the server in the display
// timezone, so they are formatted by slicing the ISO string rather than by
// going through Date — a Date would re-render them in the browser's timezone
// and quietly stop being "по Новосибирску". `retry_at` is the exception: it is
// UTC on purpose, because only the countdown needs it and that is arithmetic.

const $ = (id) => document.getElementById(id);

const POLL_ACTIVE_MS = 1000;
const POLL_IDLE_MS = 5000;

const STATUS_LABELS = {
  pending: "в очереди",
  starting: "запускается",
  running: "идёт выкачка",
  waiting_retry: "пауза",
  done: "завершён",
  failed: "ошибка",
};

const STATUS_BADGES = {
  pending: "badge--running",
  starting: "badge--running",
  running: "badge--running",
  waiting_retry: "badge--waiting",
  done: "badge--done",
  failed: "badge--failed",
};

const DIGITS = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];

// --- transport --------------------------------------------------------------

async function request(path, options) {
  const response = await fetch(path, options);
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    throw new Error(describeError(payload, response));
  }
  return payload;
}

function describeError(payload, response) {
  const detail = payload && payload.detail;
  if (typeof detail === "string") return detail;
  // FastAPI reports validation errors as a list of objects.
  if (Array.isArray(detail) && detail.length) {
    return detail.map((item) => item.msg || JSON.stringify(item)).join("; ");
  }
  return `${response.status} ${response.statusText}`;
}

function postJson(path, body) {
  return request(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

// --- formatting -------------------------------------------------------------

const ISO_PARTS = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/;

function formatNsk(iso) {
  if (!iso) return "—";
  const parts = ISO_PARTS.exec(iso);
  if (!parts) return iso;
  const [, year, month, day, hour, minute, second] = parts;
  return `${day}.${month}.${year} ${hour}:${minute}:${second}`;
}

function formatTimeNsk(iso) {
  const parts = ISO_PARTS.exec(iso || "");
  return parts ? `${parts[4]}:${parts[5]}:${parts[6]}` : "";
}

function formatCountdown(milliseconds) {
  const total = Math.max(0, Math.ceil(milliseconds / 1000));
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function plural(count, one, few, many) {
  const mod100 = count % 100;
  const mod10 = count % 10;
  if (mod100 >= 11 && mod100 <= 14) return many;
  if (mod10 === 1) return one;
  if (mod10 >= 2 && mod10 <= 4) return few;
  return many;
}

function showError(element, message) {
  element.textContent = message;
  element.hidden = !message;
}

// --- screen 1: download -----------------------------------------------------

const downloadScreen = {
  run: null,
  pollTimer: null,
  tickTimer: null,
  starting: false,
  demoModeAllowed: false,

  init() {
    $("start").addEventListener("click", () => this.start());
    // Ticks between polls so the countdown moves every second, not once a second
    // per network round trip.
    this.tickTimer = setInterval(() => this.renderWaiting(), 1000);
    this.loadConfig();
    this.poll();
  },

  async loadConfig() {
    // The switch is drawn only where the deployment allows it. Failing to ask is
    // not worth a message: the page works, it simply offers no demo run.
    try {
      const config = await request("/api/config");
      this.demoModeAllowed = config.demo_mode === true;
      $("demo-switch").hidden = !this.demoModeAllowed;
      if (!this.demoModeAllowed) $("demo-mode").checked = false;
    } catch {
      this.demoModeAllowed = false;
      $("demo-switch").hidden = true;
      $("demo-mode").checked = false;
    }
  },

  async start() {
    if (this.starting) return;
    this.starting = true;
    $("start").disabled = true;
    // Cleared only here: a deliberate start is the one thing that may drop the
    // previous start's complaint. Background polling must never do it — the
    // reason the button refused would vanish a second later on its own.
    showError($("start-error"), "");
    try {
      this.run = await postJson("/api/runs", {
        demo: this.demoModeAllowed && $("demo-mode").checked,
      });
      this.render();
    } catch (error) {
      showError($("start-error"), error.message);
    } finally {
      this.starting = false;
      this.schedule(0);
    }
  },

  async poll() {
    try {
      // Also the service's recovery path: every poll makes the server close a
      // run abandoned by a dead worker, which is what unblocks the button.
      this.run = await request("/api/runs/current");
      // Success clears the polling complaint and nothing else.
      showError($("poll-error"), "");
      this.render();
    } catch (error) {
      // The run itself is left on screen: losing one poll says nothing about it,
      // and blanking the card would look like the run disappeared.
      showError($("poll-error"), `состояние не обновляется: ${error.message}`);
    }
    this.schedule();
  },

  schedule(delay) {
    clearTimeout(this.pollTimer);
    const wait = delay !== undefined ? delay : this.isActive() ? POLL_ACTIVE_MS : POLL_IDLE_MS;
    this.pollTimer = setTimeout(() => this.poll(), wait);
  },

  isActive() {
    return Boolean(this.run && this.run.active);
  },

  render() {
    const run = this.run;
    const badge = $("run-status");

    if (!run) {
      badge.textContent = "процесс не запускался";
      badge.className = "badge badge--idle";
      $("run-demo").hidden = true;
      $("start").disabled = this.starting;
      $("run-card").hidden = true;
      $("log-card").hidden = true;
      return;
    }

    badge.textContent = STATUS_LABELS[run.status] || run.status;
    badge.className = `badge ${STATUS_BADGES[run.status] || "badge--idle"}`;
    // Says what the numbers below actually describe: real files or the stub's.
    $("run-demo").hidden = !run.demo;
    // The button stays disabled for as long as a run occupies the slot; the
    // server would answer 409 anyway.
    $("start").disabled = run.active || this.starting;

    $("run-card").hidden = false;
    $("started-at").textContent = formatNsk(run.started_at_nsk);
    $("finished-at").textContent = run.finished_at_nsk
      ? formatNsk(run.finished_at_nsk)
      : run.active
        ? "ещё идёт"
        : "—";

    const seen = run.names_seen;
    const saved = run.files_saved;
    $("progress-text").textContent =
      `получено ${seen} ${plural(seen, "название", "названия", "названий")} файлов, ` +
      `скачано ${saved} из ${seen}`;
    this.renderProgressBar(run.status);

    showError($("run-error"), run.status === "failed" && run.error ? run.error : "");
    this.renderWaiting();
    this.renderLog(run.events || []);
  },

  renderProgressBar(status) {
    const track = $("progress-track");
    const bar = $("progress-bar");
    const active = ["pending", "starting", "running", "waiting_retry"].includes(status);

    track.classList.toggle("progress--indeterminate", active);
    track.classList.toggle("progress--failed", status === "failed");
    track.setAttribute("aria-busy", String(active));

    if (status === "done") {
      bar.style.width = "100%";
      track.setAttribute("aria-valuenow", "100");
    } else {
      bar.style.width = active ? "35%" : "0";
      track.removeAttribute("aria-valuenow");
    }
  },

  renderWaiting() {
    const run = this.run;
    const waiting = $("waiting");
    if (!run || run.status !== "waiting_retry" || !run.retry_at) {
      waiting.hidden = true;
      return;
    }
    waiting.hidden = false;
    $("countdown").textContent = formatCountdown(new Date(run.retry_at) - Date.now());
    $("retry-reason").textContent = run.retry_reason || "";
  },

  renderLog(events) {
    const list = $("log");
    $("log-card").hidden = events.length === 0;
    // Stick to the bottom only when the reader is already there, so scrolling
    // back through the log is not yanked away on the next poll.
    const atBottom = list.scrollHeight - list.scrollTop - list.clientHeight < 40;

    list.innerHTML = "";
    for (const event of events) {
      const item = document.createElement("li");
      const time = document.createElement("time");
      time.textContent = formatTimeNsk(event.ts_nsk);
      const message = document.createElement("span");
      message.className = `level--${event.level}`;
      message.textContent = event.message;
      item.append(time, message);
      list.append(item);
    }
    if (atBottom) list.scrollTop = list.scrollHeight;
  },
};

// --- screen 2: files and statistics ----------------------------------------

const filesScreen = {
  page: 1,
  size: 20,
  order: "desc",
  total: 0,
  totalPages: 0,
  items: [],
  // The selection being edited right now — the *future* request. It has nothing
  // to do with the result already on screen; see `stats.snapshot`.
  selected: new Set(),
  selectAll: false,

  // Two independent generation counters, one per operation. A single shared one
  // would let a finished file listing cancel a statistics request that has
  // nothing to do with it. Only a response whose generation is still current may
  // touch state or the DOM — stale successes and stale errors are both dropped.
  filesGeneration: 0,

  // `snapshot` is the selection the displayed result describes: `{selectAll,
  // fileIds, asOf}`, frozen when «Произвести расчёты» was pressed and never
  // re-read from the checkboxes afterwards. That is what lets the user carry on
  // editing the selection while an earlier result stays open and pageable — its
  // pages keep asking for the same files as of the same moment.
  stats: { snapshot: null, page: 1, size: 20, totalPages: 0, generation: 0 },

  init() {
    $("files-size").addEventListener("change", (event) => {
      this.size = Number(event.target.value);
      this.page = 1;
      this.load();
    });
    $("files-order").addEventListener("change", (event) => {
      this.order = event.target.value;
      this.page = 1;
      this.load();
    });
    $("files-prev").addEventListener("click", () => this.turn(-1));
    $("files-next").addEventListener("click", () => this.turn(1));

    $("select-page").addEventListener("change", (event) => this.selectPage(event.target.checked));
    $("select-all").addEventListener("change", (event) => this.setSelectAll(event.target.checked));
    $("clear-selection").addEventListener("click", () => this.clearSelection());
    $("calculate").addEventListener("click", () => this.calculate());

    $("stats-prev").addEventListener("click", () => this.turnStats(-1));
    $("stats-next").addEventListener("click", () => this.turnStats(1));
  },

  async load() {
    const generation = ++this.filesGeneration;
    try {
      const page = await request(
        `/api/files?page=${this.page}&size=${this.size}&order=${this.order}`
      );
      // Answers can arrive out of order: switching the sort while the previous
      // request is in flight would otherwise repaint the table with rows that
      // belong to the sort the controls no longer show.
      if (generation !== this.filesGeneration) return;
      this.items = page.items;
      this.total = page.total;
      this.totalPages = page.total_pages;
      this.render();
    } catch (error) {
      if (generation !== this.filesGeneration) return;
      $("files-body").innerHTML = "";
      const row = $("files-body").insertRow();
      const cell = row.insertCell();
      cell.colSpan = 3;
      cell.className = "error";
      cell.textContent = error.message;
    }
  },

  turn(step) {
    const next = this.page + step;
    if (next < 1 || (this.totalPages && next > this.totalPages)) return;
    this.page = next;
    this.load();
  },

  render() {
    const body = $("files-body");
    body.innerHTML = "";

    if (this.items.length === 0) {
      const row = body.insertRow();
      const cell = row.insertCell();
      cell.colSpan = 3;
      cell.className = "muted";
      cell.textContent = "файлов пока нет — запустите скачивание";
    }

    for (const file of this.items) {
      const row = body.insertRow();
      const check = row.insertCell();
      check.className = "cell--check";
      const box = document.createElement("input");
      box.type = "checkbox";
      box.checked = this.selectAll || this.selected.has(file.id);
      box.disabled = this.selectAll;
      box.addEventListener("change", () => {
        if (box.checked) this.selected.add(file.id);
        else this.selected.delete(file.id);
        // Editing the future selection, not the shown result: the open
        // statistics keeps describing what it was asked for.
        this.renderSelection();
      });
      check.append(box);
      row.insertCell().textContent = file.name;
      row.insertCell().textContent = formatNsk(file.downloaded_at_nsk);
    }

    $("files-total").textContent = `всего файлов: ${this.total}`;
    $("files-page").textContent = this.totalPages
      ? `страница ${this.page} из ${this.totalPages}`
      : "страниц нет";
    $("files-prev").disabled = this.page <= 1;
    $("files-next").disabled = !this.totalPages || this.page >= this.totalPages;
    this.renderSelection();
  },

  renderSelection() {
    const info = $("selection-info");
    if (this.selectAll) {
      info.textContent = `выбраны все файлы в базе (${this.total})`;
    } else if (this.selected.size) {
      const count = this.selected.size;
      info.textContent = `выбрано ${count} ${plural(count, "файл", "файла", "файлов")}`;
    } else {
      info.textContent = "ничего не выбрано";
    }

    const pageIds = this.items.map((file) => file.id);
    $("select-page").checked =
      !this.selectAll && pageIds.length > 0 && pageIds.every((id) => this.selected.has(id));
    $("select-page").disabled = this.selectAll || pageIds.length === 0;
    $("calculate").disabled = !this.selectAll && this.selected.size === 0;

    // The result stays; it just stops matching the checkboxes. Saying so is the
    // whole difference between "stale" and "wrong".
    $("stats-stale").hidden = this.selectionMatchesShown();
  },

  selectPage(checked) {
    for (const file of this.items) {
      if (checked) this.selected.add(file.id);
      else this.selected.delete(file.id);
    }
    this.render();
  },

  setSelectAll(checked) {
    this.selectAll = checked;
    this.render();
  },

  clearSelection() {
    this.selected.clear();
    this.selectAll = false;
    $("select-all").checked = false;
    this.render();
  },

  /** True when the shown result still describes what the checkboxes say. */
  selectionMatchesShown() {
    const snapshot = this.stats.snapshot;
    if (!snapshot) return true;
    if (snapshot.selectAll !== this.selectAll) return false;
    if (snapshot.selectAll) return true;
    if (snapshot.fileIds.length !== this.selected.size) return false;
    return snapshot.fileIds.every((id) => this.selected.has(id));
  },

  async calculate() {
    // The one place a snapshot is born. From here the checkboxes may change
    // freely: this result keeps describing what was asked for, cut-off included.
    // `asOf` starts empty — the server picks it and answers with it, and every
    // later page of this snapshot sends the same one back.
    this.stats.snapshot = {
      selectAll: this.selectAll,
      fileIds: this.selectAll ? [] : [...this.selected],
      asOf: null,
    };
    this.stats.page = 1;
    this.stats.totalPages = 0;
    await this.loadStats();
  },

  async loadStats() {
    const snapshot = this.stats.snapshot;
    // No snapshot means nothing has been asked for yet: the pager must not
    // invent a request of its own.
    if (!snapshot) return;

    // Bumped per request, so the answer to a superseded calculation — or to a
    // page the user has already clicked past — cannot repaint the card.
    const generation = ++this.stats.generation;
    const body = { page: this.stats.page, size: this.stats.size };
    if (snapshot.selectAll) body.select_all = true;
    else body.file_ids = snapshot.fileIds;
    if (snapshot.asOf) body.as_of = snapshot.asOf;

    showError($("stats-error"), "");
    try {
      const result = await postJson("/api/stats", body);
      if (generation !== this.stats.generation) return;
      // Stored on the snapshot, not on the screen state: the cut-off belongs to
      // this selection and travels with it.
      snapshot.asOf = result.as_of;
      this.stats.totalPages = result.per_file.total_pages;
      this.renderStats(result);
      $("stats-card").hidden = false;
      this.renderSelection();
    } catch (error) {
      if (generation !== this.stats.generation) return;
      $("stats-card").hidden = false;
      showError($("stats-error"), error.message);
    }
  },

  turnStats(step) {
    if (!this.stats.snapshot) return;
    const next = this.stats.page + step;
    if (next < 1 || (this.stats.totalPages && next > this.stats.totalPages)) return;
    this.stats.page = next;
    this.loadStats();
  },

  renderStats(result) {
    const count = result.selected_count;
    $("stats-scope").textContent =
      `${count} ${plural(count, "файл", "файла", "файлов")} в выборке, ` +
      `отсечка ${formatNsk(nskFromUtc(result.as_of))}`;

    const totalHead = $("total-head");
    const totalBody = $("total-body");
    totalHead.innerHTML = "";
    totalBody.innerHTML = "";
    totalHead.append(headCell("Цифра"));
    totalBody.append(bodyCell("Всего"));
    for (const digit of DIGITS) {
      totalHead.append(headCell(digit));
      totalBody.append(bodyCell(String(result.total[digit] ?? 0)));
    }

    const perFileHead = $("per-file-head");
    const perFileBody = $("per-file-body");
    perFileHead.innerHTML = "";
    perFileBody.innerHTML = "";
    perFileHead.append(headCell("Файл"), headCell("Скачан"));
    for (const digit of DIGITS) perFileHead.append(headCell(digit));

    for (const item of result.per_file.items) {
      const row = perFileBody.insertRow();
      row.insertCell().textContent = item.name;
      row.insertCell().textContent = formatNsk(item.downloaded_at_nsk);
      for (const digit of DIGITS) {
        row.insertCell().textContent = String(item.counts[digit] ?? 0);
      }
    }

    $("stats-page").textContent = this.stats.totalPages
      ? `страница ${result.per_file.page} из ${this.stats.totalPages}`
      : "страниц нет";
    $("stats-prev").disabled = this.stats.page <= 1;
    $("stats-next").disabled = !this.stats.totalPages || this.stats.page >= this.stats.totalPages;
  },
};

function headCell(text) {
  const cell = document.createElement("th");
  cell.textContent = text;
  return cell;
}

function bodyCell(text) {
  const cell = document.createElement("td");
  cell.textContent = text;
  return cell;
}

function nskFromUtc(iso) {
  // `as_of` travels in UTC. Rendered through the display timezone explicitly,
  // so it does not depend on where the browser thinks it is.
  const formatter = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Asia/Novosibirsk",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  return formatter.format(new Date(iso)).replace(" ", "T");
}

// --- routing ----------------------------------------------------------------

function showScreen() {
  const name = location.hash === "#/files" ? "files" : "download";
  for (const screen of ["download", "files"]) {
    $(`screen-${screen}`).classList.toggle("screen--active", screen === name);
  }
  for (const tab of document.querySelectorAll(".tab")) {
    tab.classList.toggle("tab--active", tab.dataset.screen === name);
  }
  // Refreshed on every entry, not once: a run finishing on the other screen is
  // exactly when the list goes stale. Page, size, order and the selection are
  // left alone — the user's work is not what went out of date. A first attempt
  // that failed is retried here too, instead of leaving the screen empty until
  // a full page reload.
  if (name === "files") filesScreen.load();
}

window.addEventListener("hashchange", showScreen);

downloadScreen.init();
filesScreen.init();
showScreen();
