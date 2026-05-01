import crypto from "node:crypto";

function timingSafeEqualString(expected, supplied) {
  if (
    typeof expected !== "string" ||
    typeof supplied !== "string" ||
    expected.length !== supplied.length
  ) {
    return false;
  }
  try {
    return crypto.timingSafeEqual(
      Buffer.from(expected, "utf8"),
      Buffer.from(supplied, "utf8")
    );
  } catch {
    return false;
  }
}

/**
 * Requires `EXTERNAL_API_KEY`. Client sends either header `X-API-Key: <key>`
 * or `Authorization: Bearer <key>`.
 */
export function requireExternalApiKey(req, res, next) {
  const configured = process.env.EXTERNAL_API_KEY?.trim();
  if (!configured) {
    res
      .status(503)
      .json({ ok: false, error: "EXTERNAL_API_KEY is not configured on server" });
    return;
  }

  const bearer = /^Bearer\s+(\S+)/i.exec(req.get("authorization") ?? "");
  const supplied =
    (req.get("x-api-key") ?? bearer?.[1] ?? "").trim();

  if (!timingSafeEqualString(configured, supplied)) {
    res.status(401).json({ ok: false, error: "Unauthorized" });
    return;
  }

  next();
}
