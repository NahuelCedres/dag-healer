/**
 * Focus after a move inside the console: the new view's heading takes focus (and is read out), so
 * keyboard and screen-reader users continue from the top of what they opened.
 */
import { createContext, type RefObject, useContext, useEffect, useRef } from "react";

/** Incremented by the shell on every move; 0 until the first one (the page load keeps focus). */
export const MoveContext = createContext(0);

/** Focus `ref` once it is `ready` after each move. */
export const useHeadingFocus = (ref: RefObject<HTMLElement | null>, ready: boolean): void => {
  const moves = useContext(MoveContext);
  const done = useRef(0);

  useEffect(() => {
    if (moves > done.current && ready && ref.current !== null) {
      done.current = moves;
      ref.current.focus({ preventScroll: false });
    }
  }, [moves, ready, ref]);
};
