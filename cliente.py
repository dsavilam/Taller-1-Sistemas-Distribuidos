import argparse, sys
from utils import abrir_cliente, enviar_json, recibir_json

TAMANO_REQUERIDO = 5


def leer_arreglo_usuario(nombre_arreglo: str, tam_esperado: int = TAMANO_REQUERIDO):
    print(f"Ingrese {tam_esperado} enteros para el {nombre_arreglo}, separados por espacios:")
    linea = input().strip()
    if not linea:
        print(f"No ingresaste valores para {nombre_arreglo}.")
        sys.exit(1)

    partes = linea.split()
    if len(partes) > tam_esperado:
        print("Te pedi ingresar 5 numeros tontin, ingresaste mas y el arreglo se desbordo")
        sys.exit(1)
    if len(partes) < tam_esperado:
        print(f"Ingresaste menos de {tam_esperado} números para {nombre_arreglo}.")
        sys.exit(1)

    try:
        numeros = [int(x) for x in partes]
    except ValueError:
        print("Entrada inválida: asegúrate de ingresar solo enteros separados por espacios.")
        sys.exit(1)
    return numeros

def principal():
    """Punto de entrada del cliente: solicita dos arreglos tamaño 5 y envía la suma al coordinador."""
    ap = argparse.ArgumentParser(description="Cliente de suma distribuida")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    args = ap.parse_args()

    arreglo_izquierdo = leer_arreglo_usuario("primer arreglo (A)")
    arreglo_derecho   = leer_arreglo_usuario("segundo arreglo (B)")

    print(f"[cliente] A={arreglo_izquierdo}")
    print(f"[cliente] B={arreglo_derecho}")

    with abrir_cliente(args.host, args.port, timeout=10.0) as sock:
        enviar_json(sock, {"type": "sum_arrays", "a": arreglo_izquierdo, "b": arreglo_derecho})
        respuesta = recibir_json(sock, timeout=20.0)

    if respuesta.get("type") != "ok":
        mensaje_error = respuesta.get("error") or str (respuesta)
        print(f"[cliente] Error: {mensaje_error}")
        return

    resultado_suma = respuesta["result"]
    tiempo_seg = float(respuesta.get("elapsed", 0.0))
    operadores_info = respuesta.get("operadores", [])

    esperado_local = [v1 + v2 for v1, v2 in zip(arreglo_izquierdo, arreglo_derecho)]
    ok = (esperado_local == resultado_suma)

    print(f"[cliente] OK={ok} | tiempo={tiempo_seg:.4f}s")
    print(f"[cliente] Resultado={resultado_suma}")
    print("[cliente] Operadores:")
    for op in operadores_info:
        if isinstance(op, dict):
            estado = "VIVO" if op.get("vivo") else "DOWN"
            nombre = op.get("nombre", "?")
            host = op.get("host", "?")
            puerto = op.get("puerto", "?")
            print(f"  - {nombre} @ {host}:{puerto} -> {estado}")

if __name__ == "__main__":
    principal()
