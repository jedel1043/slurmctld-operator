from typing import Iterator, Optional, Iterable, TypeVar, Tuple

T = TypeVar('T')

def _peekable(items: Iterable[T]) -> Iterator[Tuple[T, Optional[T]]]:
    """Iterate through the items, peeking the next item after the currently yielded item."""
    items_iter = iter(items)
    prev = next(items_iter, None)

    for item in items_iter:
        yield prev, item
        prev = item
    
    if prev is not None:
        yield prev, None

def _lower_upper(left: str, right: str) -> bool:
    return left.islower() and right.isupper()

def _acronym(left: str, middle: str, right: str) -> bool:
    return left.isupper() and middle.isupper() and right.islower()

def _split_camel_case(input: str) -> Iterator[str]:
    # Adapted from https://docs.rs/heck/latest/src/heck/lib.rs.html
    """
    Split a CamelCase string into the list of its words.

    The input is only split using the lower-upper rule and the acronym rule:
    - Lower-upper rule: a lowercase letter followed by an uppercase letter:
      "oneThing" -> ["one", "Thing"]
    - Acronym rule: Two or more uppercase letters followed by a lowercase letter:
      "ABCThing" -> ["ABC", "Thing"]
    """
    init = 0
    prev_was_upper = False
    for (i, curr), peek in _peekable(enumerate(input)):
        if peek is None:
            # No further word boundaries, so we can return now.
            yield input[init:]
            return
        next_i, peek = peek

        if curr.islower() and peek.isupper():
            yield input[init:next_i]
            init = next_i
        elif prev_was_upper and curr.isupper() and peek.islower():
            yield input[init:i]
            init = i
        prev_was_upper = curr.isupper()

words = _split_camel_case("TOOManyWordsINThisWrdCPU")

print(list(words))