import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: __ENV.VUS ? Number(__ENV.VUS) : 50,
  duration: __ENV.DURATION || "2m",
};

const BASE_URL = (__ENV.BASE_URL || "http://127.0.0.1:8001").replace(/\/$/, "");
const COOKIE = __ENV.COOKIE || "";

export default function () {
  const headers = {
    "Content-Type": "application/json",
  };
  if (COOKIE) headers["Cookie"] = COOKIE;

  const payload = JSON.stringify({ custom_factors: [] });
  const r = http.post(`${BASE_URL}/api/latest_enriched`, payload, { headers });
  check(r, {
    "status 200": (x) => x.status === 200,
    "json ok": (x) => {
      try {
        const j = x.json();
        return j && j.ok === true && Array.isArray(j.results);
      } catch {
        return false;
      }
    },
  });

  sleep(0.2);
}

