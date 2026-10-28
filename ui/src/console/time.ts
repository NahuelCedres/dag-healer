/** Times as people read them: "4 min ago", with the exact time on hover. */

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const pad = (value: number): string => String(value).padStart(2, "0");

const parse = (value: string | null | undefined): Date | undefined => {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }
  const date = new Date(value);

  return Number.isNaN(date.getTime()) ? undefined : date;
};

/** `2026-10-17 14:03` in the browser's time zone ("" when missing). */
export const exactTime = (value: string | null | undefined): string => {
  const date = parse(value);

  return date === undefined
    ? ""
    : `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
        `${pad(date.getHours())}:${pad(date.getMinutes())}`;
};

/** "just now", "4 min ago", "3 h ago", "2 days ago", then "Oct 17" ("" when missing). */
export const relativeTime = (value: string | null | undefined, now: number = Date.now()): string => {
  const date = parse(value);

  if (date === undefined) {
    return "";
  }
  const elapsed = now - date.getTime();

  if (elapsed < MINUTE) {
    return "just now";
  }
  if (elapsed < HOUR) {
    return `${Math.floor(elapsed / MINUTE)} min ago`;
  }
  if (elapsed < DAY) {
    return `${Math.floor(elapsed / HOUR)} h ago`;
  }
  if (elapsed < 7 * DAY) {
    const days = Math.floor(elapsed / DAY);

    return days === 1 ? "1 day ago" : `${days} days ago`;
  }

  return `${MONTHS[date.getMonth()] ?? ""} ${date.getDate()}`;
};

/** The short form for narrow places: "4m", "3h", "2d", "Oct 17". */
export const shortAge = (value: string | null | undefined, now: number = Date.now()): string => {
  const date = parse(value);

  if (date === undefined) {
    return "";
  }
  const elapsed = Math.max(0, now - date.getTime());

  if (elapsed < HOUR) {
    return `${Math.max(1, Math.floor(elapsed / MINUTE))}m`;
  }
  if (elapsed < DAY) {
    return `${Math.floor(elapsed / HOUR)}h`;
  }
  if (elapsed < 7 * DAY) {
    return `${Math.floor(elapsed / DAY)}d`;
  }

  return `${MONTHS[date.getMonth()] ?? ""} ${date.getDate()}`;
};
