"""Ponto de entrada com nome de módulo válido para AWS Lambda."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


_CAMINHO_LEITOR = Path(__file__).with_name("script-leitura-hardware.py")
_SPEC_LEITOR = spec_from_file_location(
    "leitor_hardware_infrawatch", _CAMINHO_LEITOR
)
if _SPEC_LEITOR is None or _SPEC_LEITOR.loader is None:
    raise ImportError(f"Não foi possível carregar {_CAMINHO_LEITOR.name}.")

_LEITOR = module_from_spec(_SPEC_LEITOR)
_SPEC_LEITOR.loader.exec_module(_LEITOR)

lambda_handler = _LEITOR.lambda_handler
