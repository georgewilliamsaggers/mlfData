import OpenAI from "openai";
import { getPgSchema, runQuery } from "../database/hobbiton/index.js";
import { isReadOnlySelect } from "../lib/sqlGuard.js";

function buildChatSystemPrompt() {
  const s = getPgSchema();
  return `You are a data assistant for a PostgreSQL read-only partner database.
Schema ${s} has views: integration_accounts, integration_clients, integration_transactions, integration_loans.
Always use ${s}-qualified names (e.g. ${s}.integration_clients).
Only SELECT (or WITH … SELECT) queries are allowed. Prefer aggregates and LIMIT for exploration.`;
}

export async function chatWithAgent(userMessage, history = []) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is not set");
  }

  const openai = new OpenAI({ apiKey });

  const messages = [
    { role: "system", content: buildChatSystemPrompt() },
    ...history.map((m) => ({ role: m.role, content: m.content })),
    { role: "user", content: userMessage },
  ];

  const tools = [
    {
      type: "function",
      function: {
        name: "run_sql",
        description:
          "Execute a single read-only SQL query against the database. Must be SELECT or WITH … SELECT only.",
        parameters: {
          type: "object",
          properties: {
            sql: { type: "string", description: "The SQL to run" },
          },
          required: ["sql"],
        },
      },
    },
  ];

  let completion = await openai.chat.completions.create({
    model: process.env.OPENAI_MODEL || "gpt-4o-mini",
    messages,
    tools,
    tool_choice: "auto",
  });

  let msg = completion.choices[0]?.message;
  if (!msg) {
    return { reply: "No response from model.", toolResults: [] };
  }

  const toolResults = [];
  let toolRounds = 0;
  const maxToolRounds = 5;

  while (msg.tool_calls?.length && toolRounds < maxToolRounds) {
    toolRounds += 1;
    messages.push(msg);

    for (const call of msg.tool_calls) {
      if (call.type !== "function" || call.function.name !== "run_sql") continue;

      let args;
      try {
        args = JSON.parse(call.function.arguments || "{}");
      } catch {
        toolResults.push({ error: "Invalid tool arguments" });
        messages.push({
          role: "tool",
          tool_call_id: call.id,
          content: JSON.stringify({ error: "Invalid JSON in arguments" }),
        });
        continue;
      }

      const sql = args.sql;
      if (!sql || typeof sql !== "string") {
        messages.push({
          role: "tool",
          tool_call_id: call.id,
          content: JSON.stringify({ error: "Missing sql string" }),
        });
        continue;
      }

      if (!isReadOnlySelect(sql)) {
        messages.push({
          role: "tool",
          tool_call_id: call.id,
          content: JSON.stringify({
            error: "Only read-only SELECT queries are allowed.",
          }),
        });
        continue;
      }

      try {
        const result = await runQuery(sql);
        const preview = {
          rowCount: result.rowCount,
          rows: result.rows.slice(0, 200),
          truncated: result.rows.length > 200,
        };
        toolResults.push({ sql, ...preview });
        messages.push({
          role: "tool",
          tool_call_id: call.id,
          content: JSON.stringify(preview),
        });
      } catch (err) {
        toolResults.push({ sql, error: err.message });
        messages.push({
          role: "tool",
          tool_call_id: call.id,
          content: JSON.stringify({ error: err.message }),
        });
      }
    }

    completion = await openai.chat.completions.create({
      model: process.env.OPENAI_MODEL || "gpt-4o-mini",
      messages,
      tools,
      tool_choice: "auto",
    });
    msg = completion.choices[0]?.message;
    if (!msg) break;
  }

  const reply = msg?.content?.trim() || "(no text reply)";
  return { reply, toolResults };
}
