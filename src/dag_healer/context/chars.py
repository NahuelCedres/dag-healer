"""Characters that show nothing, and letters and brackets that look like others.

:func:`strip_invisible` takes out of a context file, right after it is decoded, every character
that shows nothing: the format characters (Unicode category Cf: zero-width spaces and joiners, the
word joiner, bidi controls, soft hyphens, byte order marks, the tag characters of U+E0000) and the
other default-ignorable ones (the combining grapheme joiner, variation selectors, Hangul fillers).
Inside a secret, an e-mail address or a prompt tag they would hide it from the checks and the
masking while the model still reads through them. A zero-width joiner or non-joiner between two
characters that are not ASCII (an emoji sequence, Persian, Devanagari) stays, and so does a
variation selector after one: the text needs them, and no check reads ASCII through them. The
secret check, the parsing, the masking, the store and the prompt then all see the same text.

:func:`detection_copy` is what the prompt-tag check reads
(:func:`dag_healer.context.prompt.neutralise_tags`): the text without any invisible character or
combining mark, each character in its compatibility form (NFKD: fullwidth and mathematical letters
are ASCII), a tag character as the ASCII one it spells, and letters and brackets that look like
Latin ones (a Cyrillic i, a Greek omicron, a mathematical angle bracket) as those, case-folded,
with the position in the text of each character of the copy.
"""

from __future__ import annotations

import unicodedata

#: Default-ignorable code points outside the format characters (category Cf).
_OTHER_IGNORABLE: tuple[tuple[int, int], ...] = (
    (0x034F, 0x034F),  # combining grapheme joiner
    (0x115F, 0x1160),  # Hangul choseong and jungseong fillers
    (0x17B4, 0x17B5),  # Khmer inherent vowels
    (0x180B, 0x180F),  # Mongolian free variation selectors and vowel separator
    (0x2060, 0x206F),  # word joiner, invisible operators, deprecated format characters
    (0x3164, 0x3164),  # Hangul filler
    (0xFE00, 0xFE0F),  # variation selectors
    (0xFFA0, 0xFFA0),  # halfwidth Hangul filler
    (0xE0000, 0xE0FFF),  # tag characters, variation selectors supplement
)
#: Joiners that a script or an emoji sequence needs between two characters.
_JOINERS = frozenset("\u200c\u200d")
#: Selectors that choose the form of the character before them.
_SELECTORS: tuple[tuple[int, int], ...] = (
    (0xFE00, 0xFE0F),
    (0xE0100, 0xE01EF),
    (0x180B, 0x180D),
    (0x180F, 0x180F),
)
#: The tag characters that spell ASCII (U+E0020 to U+E007E spell " " to "~").
_TAG_SPELLING = (0xE0020, 0xE007E)
#: Characters a reader can take for an ASCII one (beyond what NFKD gives), lower case.
_LOOKALIKES: dict[str, str] = {
    # brackets and slashes
    **dict.fromkeys("\u2039\u02c2\u1438\u3008\u27e8\u276e\u29fc\u300a\u227a\u276c\u2770", "<"),
    **dict.fromkeys("\u2044\u2215\u2571\u29f8", "/"),
    **dict.fromkeys("\u2017\u02cd", "_"),
    # Cyrillic
    "\u0430": "a", "\u0410": "a", "\u0412": "b", "\u0441": "c", "\u0421": "c", "\u0501": "d", "\u0435": "e",
    "\u0415": "e", "\u04bb": "h", "\u041d": "h", "\u0456": "i", "\u0406": "i", "\u04c0": "i", "\u0458": "j",
    "\u0408": "j", "\u041a": "k", "\u04cf": "l", "\u041c": "m", "\u043e": "o", "\u041e": "o", "\u0440": "p",
    "\u0420": "p", "\u051b": "q", "\u0455": "s", "\u0405": "s", "\u0422": "t", "\u0443": "y", "\u04ae": "y",
    "\u0445": "x", "\u0425": "x", "\u051d": "w", "\u0475": "v",
    # Greek
    "\u03b1": "a", "\u0391": "a", "\u0392": "b", "\u03f2": "c", "\u03f9": "c", "\u0395": "e", "\u0396": "z",
    "\u0397": "h", "\u03b9": "i", "\u0399": "i", "\u03f3": "j", "\u037f": "j", "\u03ba": "k", "\u039a": "k",
    "\u039c": "m", "\u039d": "n", "\u03bd": "v", "\u03bf": "o", "\u039f": "o", "\u03c1": "p", "\u03a1": "p",
    "\u03a4": "t", "\u03c5": "u", "\u03a5": "y", "\u03c7": "x", "\u03a7": "x",
    # Latin letters of other shapes, Armenian
    "\u0131": "i", "\u0237": "j", "\u0269": "i", "\u026a": "i", "\u029f": "l", "\u0274": "n", "\ua731": "s",
    "\u1d04": "c", "\u1d05": "d", "\u1d07": "e", "\u1d0f": "o", "\u1d1b": "t", "\u1d1c": "u", "\u1d20": "v",
    "\u1d22": "z", "\u0251": "a", "\u0261": "g", "\u0585": "o", "\u057d": "u",
}  # fmt: skip


def is_invisible(char: str) -> bool:
    """True for a character that shows nothing: a format character or another default-ignorable one."""
    if unicodedata.category(char) == "Cf":
        return True
    code = ord(char)
    return any(low <= code <= high for low, high in _OTHER_IGNORABLE)


def _is_selector(char: str) -> bool:
    code = ord(char)
    return any(low <= code <= high for low, high in _SELECTORS)


def _stays(text: str, index: int) -> bool:
    """True for an invisible character that the text needs where it is (see the module docstring)."""
    char = text[index]
    before = text[index - 1] if index > 0 else ""
    if char in _JOINERS:
        after = text[index + 1] if index + 1 < len(text) else ""
        return bool(before) and bool(after) and not before.isascii() and not after.isascii()
    return _is_selector(char) and bool(before) and not before.isascii()


def strip_invisible(text: str) -> str:
    """``text`` without the characters that show nothing (the module docstring says which stay)."""
    if text.isascii():
        return text
    return "".join(char for index, char in enumerate(text) if not is_invisible(char) or _stays(text, index))


def detection_copy(text: str) -> tuple[str, list[int]]:
    """``(copy, positions)``: what a check of prompt tags reads (module docstring), and for each
    character of the copy the index in ``text`` of the character it comes from."""
    if text.isascii():
        return text.lower(), list(range(len(text)))
    chars: list[str] = []
    positions: list[int] = []
    for index, char in enumerate(text):
        code = ord(char)
        if _TAG_SPELLING[0] <= code <= _TAG_SPELLING[1]:
            chars.append(chr(code - 0xE0000).lower())
            positions.append(index)
            continue
        if is_invisible(char):
            continue
        for part in unicodedata.normalize("NFKD", char):
            if unicodedata.category(part).startswith("M"):
                continue  # a combining mark: <inc\u00efdent> reads as <incident>
            for folded in _LOOKALIKES.get(part, part).casefold():
                chars.append(folded)
                positions.append(index)
    return "".join(chars), positions


__all__ = ["detection_copy", "is_invisible", "strip_invisible"]
