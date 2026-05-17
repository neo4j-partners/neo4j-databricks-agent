"use strict";

const SSE_DONE = "[DONE]";
const els = {
  transcript: document.getElementById("transcript"),
  form: document.getElementById("composer"),
  input: document.getElementById("input"),
  send: document.getElementById("send"),
  status: document.getElementById("status"),
  reset: document.getElementById("reset"),
};

const state = {
  // Conversation history sent on every turn. We persist the full Responses-API
  // output items returned by the agent so tool-call context is preserved.
  history: [],
  sessionId: crypto.randomUUID(),
  inFlight: null,
};

function escapeHtml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function addBubble(role, label, opts = {}) {
  const div = document.createElement("div");
  div.className = `bubble ${role}`;
  const tag = document.createElement("span");
  tag.className = "role";
  tag.textContent = label;
  const body = document.createElement("div");
  body.className = "body";
  if (opts.cursor) body.classList.add("cursor");
  div.append(tag, body);
  els.transcript.append(div);
  div.scrollIntoView({ block: "end", behavior: "smooth" });
  return { wrapper: div, body };
}

function renderUser(text) {
  const { body } = addBubble("user", "you");
  body.textContent = text;
}

function appendDelta(body, delta) {
  body.append(document.createTextNode(delta));
  body.parentElement.scrollIntoView({ block: "end", behavior: "smooth" });
}

function renderToolCall(item) {
  const { wrapper, body } = addBubble("tool", `tool · ${item.name || "call"}`);
  const args = item.arguments || "{}";
  let pretty = args;
  try {
    pretty = JSON.stringify(JSON.parse(args), null, 2);
  } catch (_) {
    /* keep raw */
  }
  const details = document.createElement("details");
  details.className = "tool-block";
  details.open = false;
  const summary = document.createElement("summary");
  summary.textContent = "arguments";
  const pre = document.createElement("pre");
  pre.textContent = pretty;
  details.append(summary, pre);
  body.append(details);
  wrapper.dataset.callId = item.call_id || item.id || "";
  return wrapper;
}

function renderToolOutput(item) {
  const callId = item.call_id;
  const existing = callId
    ? els.transcript.querySelector(`.bubble.tool[data-call-id="${callId}"]`)
    : null;
  let body;
  if (existing) {
    body = existing.querySelector(".body");
  } else {
    const created = addBubble("tool", "tool · output");
    body = created.body;
  }
  let pretty = item.output ?? "";
  if (typeof pretty !== "string") {
    pretty = JSON.stringify(pretty, null, 2);
  } else {
    try {
      pretty = JSON.stringify(JSON.parse(pretty), null, 2);
    } catch (_) {
      /* keep raw */
    }
  }
  const details = document.createElement("details");
  details.className = "tool-block";
  details.open = false;
  const summary = document.createElement("summary");
  summary.textContent = "output";
  const pre = document.createElement("pre");
  pre.textContent = pretty;
  details.append(summary, pre);
  body.append(details);
}

function setStatus(text, kind) {
  els.status.textContent = text;
  els.status.classList.remove("ok", "bad");
  if (kind) els.status.classList.add(kind);
}

function setSending(sending) {
  els.send.disabled = sending;
  els.input.disabled = sending;
  els.send.textContent = sending ? "Sending…" : "Send";
}

async function checkHealth() {
  try {
    const r = await fetch("/health", { credentials: "same-origin" });
    if (r.ok) setStatus("ready", "ok");
    else setStatus(`degraded (${r.status})`, "bad");
  } catch {
    setStatus("offline", "bad");
  }
}

function aggregateAssistantText(events) {
  // Fallback aggregator when stream emits only output_item.done events
  // without delta events.
  const parts = [];
  for (const ev of events) {
    if (ev.type === "response.output_item.done" && ev.item?.type === "message") {
      for (const c of ev.item.content || []) {
        if (c.type === "output_text" && typeof c.text === "string") {
          parts.push(c.text);
        }
      }
    }
  }
  return parts.join("");
}

async function sendMessage(userText) {
  renderUser(userText);
  state.history.push({ role: "user", content: userText });

  const assistant = addBubble("agent", "agent", { cursor: true });
  const collectedEvents = [];
  let receivedText = "";
  let currentItemId = null;
  const newOutputItems = [];

  setSending(true);
  const controller = new AbortController();
  state.inFlight = controller;

  try {
    const resp = await fetch("/responses", {
      method: "POST",
      headers: { "content-type": "application/json", accept: "text/event-stream" },
      credentials: "same-origin",
      signal: controller.signal,
      body: JSON.stringify({
        input: state.history,
        stream: true,
        context: { conversation_id: state.sessionId },
      }),
    });

    if (!resp.ok || !resp.body) {
      const detail = await resp.text().catch(() => "");
      throw new Error(`HTTP ${resp.status} ${resp.statusText}${detail ? `: ${detail}` : ""}`);
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let idx;
      while ((idx = buffer.indexOf("\n\n")) !== -1) {
        const rawEvent = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        const dataLines = rawEvent
          .split("\n")
          .filter((l) => l.startsWith("data: "))
          .map((l) => l.slice(6));
        if (!dataLines.length) continue;
        const payload = dataLines.join("\n");
        if (payload === SSE_DONE) break;

        let event;
        try {
          event = JSON.parse(payload);
        } catch (e) {
          console.warn("malformed SSE chunk", payload, e);
          continue;
        }
        collectedEvents.push(event);

        if (event.error) {
          assistant.body.classList.remove("cursor");
          assistant.body.innerHTML = `<span class="err">${escapeHtml(event.error)}</span>`;
          continue;
        }

        switch (event.type) {
          case "response.output_text.delta": {
            assistant.body.classList.add("cursor");
            const delta = event.delta || "";
            receivedText += delta;
            appendDelta(assistant.body, delta);
            currentItemId = event.item_id || currentItemId;
            break;
          }
          case "response.output_item.added": {
            currentItemId = event.item?.id || currentItemId;
            break;
          }
          case "response.output_item.done": {
            const item = event.item;
            if (!item) break;
            newOutputItems.push(item);
            if (item.type === "function_call") {
              renderToolCall(item);
            } else if (item.type === "function_call_output") {
              renderToolOutput(item);
            } else if (item.type === "message") {
              // Already streamed in via deltas; nothing more to render.
            }
            break;
          }
          case "response.completed":
          case "response.failed":
            break;
          default:
            break;
        }
      }
    }

    if (!receivedText) {
      const fallback = aggregateAssistantText(collectedEvents);
      if (fallback) {
        assistant.body.textContent = fallback;
      }
    }
    assistant.body.classList.remove("cursor");

    // Persist new output items so multi-turn tool context is preserved.
    for (const item of newOutputItems) {
      state.history.push(item);
    }
  } catch (e) {
    assistant.body.classList.remove("cursor");
    assistant.body.innerHTML = `<span class="err">${escapeHtml(e.message || String(e))}</span>`;
  } finally {
    state.inFlight = null;
    setSending(false);
    els.input.focus();
  }
}

function resetConversation() {
  if (state.inFlight) state.inFlight.abort();
  state.history = [];
  state.sessionId = crypto.randomUUID();
  els.transcript.innerHTML = "";
  els.input.focus();
}

els.form.addEventListener("submit", (e) => {
  e.preventDefault();
  const text = els.input.value.trim();
  if (!text) return;
  els.input.value = "";
  sendMessage(text);
});

els.input.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
    e.preventDefault();
    els.form.requestSubmit();
  }
});

els.reset.addEventListener("click", resetConversation);

checkHealth();
els.input.focus();
