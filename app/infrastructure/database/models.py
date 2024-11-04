from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass
class NormRef:
    t2_key_value: str
    """ Value from table2 taht will be used """

    t2_name: str
    """ The reference table """

    t1_key_join: Union[str, tuple[str, ...]]
    """ The column in the main table to join on """

    t2_key_join: Union[str, tuple[str, ...]]
    """ The column in the ref table to join on """
