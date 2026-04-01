/**
 * Very small guard so agent / APIs only run read-only queries.
 * Extend with a parser if you need stricter rules.
 */
export function isReadOnlySelect(sql) {
  const s = sql.trim().replace(/^\(\s*/u, "").replace(/\s*\)\s*$/u, "");
  const head = s.slice(0, 20).toUpperCase();
  if (!head.startsWith("SELECT") && !head.startsWith("WITH")) {
    return false;
  }
  const forbidden = /\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b/i;
  return !forbidden.test(s);
}
