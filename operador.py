import argparse, time, socket, threading, sys
from utils import abrir_servidor, recibir_json, enviar_json

def atender_conexion(conexion: socket.socket, direccion, nombre_operador: str, delay_artificial_seg: float):
    """Atiende una conexión entrante: responde health o procesa 'compute_sum'.
    Parámetros:
      - conexion: socket aceptado ya conectado con el coordinador
      - direccion: tupla (host, puerto) del peer (informativo)
      - nombre_operador: nombre de este proceso operador
      - delay_artificial_seg: delay intencional por petición (simular carga)
    Respuestas:
      - 'health_ok' con campo 'operador' para health
      - 'result' con suma de subarreglos para compute_sum
    """
    try:
        mensaje = recibir_json(conexion, timeout=10.0)
        tipo_mensaje = mensaje.get("type")

        if tipo_mensaje == "health":
            enviar_json(conexion, {"type": "health_ok", "operador": nombre_operador})
            return

        if tipo_mensaje == "compute_sum":
            subarreglo_izquierdo = mensaje["a"]
            subarreglo_derecho  = mensaje["b"]
            indice_parte = mensaje.get("idx", 0)
            identificador_tarea = mensaje.get("task_id", "?")

        
            print(f"[{nombre_operador}] Va a resolver el chunk idx={indice_parte} id task={identificador_tarea} A={subarreglo_izquierdo} B={subarreglo_derecho}")

            if delay_artificial_seg > 0:
                time.sleep(delay_artificial_seg)

            if len(subarreglo_izquierdo) != len(subarreglo_derecho):
                raise ValueError("Subarreglos con longitudes distintas")

            resultado_parcial = [
                valor_izq + valor_der
                for valor_izq, valor_der in zip(subarreglo_izquierdo, subarreglo_derecho)
            ]
            enviar_json(
                conexion,
                {"type": "result", "task_id": identificador_tarea, "idx": indice_parte,
                 "result": resultado_parcial, "operador": nombre_operador}
            )
            return

        enviar_json(conexion, {"type": "error", "error": "unknown_message", "detail": tipo_mensaje})

    except Exception as e:
        try:
            enviar_json(conexion, {"type": "error", "error": str(e)})
        except Exception:
            pass
    finally:
        conexion.close()

def servir(host: str, puerto: int, nombre_operador: str, delay_artificial_seg: float):
    """Inicia el servidor del operador y atiende conexiones concurrentemente."""
    try:
        servidor = abrir_servidor(host, puerto)
    except Exception as e:
        print(f"[{nombre_operador}] No se pudo iniciar: {e}")
        sys.exit(1)

    print(f"[{nombre_operador}] Operador escuchando en {host}:{puerto} (delay={delay_artificial_seg}s)")
    while True:
        conexion, direccion = servidor.accept()
        threading.Thread(
            target=atender_conexion,
            args=(conexion, direccion, nombre_operador, delay_artificial_seg),
            daemon=True
        ).start()

if __name__ == "__main__":
    """Punto de entrada: parsea flags y lanza el operador."""
    ap = argparse.ArgumentParser(description="Servidor de Operación (Operador)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6001)
    ap.add_argument("--name", default="operador-1")
    ap.add_argument("--delay", type=float, default=0.0, help="delay artificial por solicitud (segundos)")
    args = ap.parse_args()
    servir(args.host, args.port, args.name, args.delay)

