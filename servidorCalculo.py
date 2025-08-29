import argparse, socket, threading, time, uuid, datetime
from typing import List, Tuple, Dict, Optional
from utils import abrir_servidor, abrir_cliente, recibir_json, enviar_json, Repetidor
from collections import Counter
import sys

class InfoTrabajador:
    """DTO para almacenar el estado de un operador (host, puerto, vivo, etc.)."""
    def __init__(self, nombre: str, host: str, puerto: int):
        """Inicializa la información del operador."""
        self.nombre = nombre
        self.host = host
        self.puerto = puerto
        self.vivo = False
        self.ultimo_ok = 0.0
        self.lock = threading.Lock()

    def __repr__(self):
        """Representación legible del operador para logs."""
        return f"<Operador {self.nombre} {self.host}:{self.puerto} alive={self.vivo}>"

class Coordinador:
    """Coordinador: recibe solicitudes, parte el trabajo y maneja fallos/health checks."""
    def __init__(self, host: str, puerto: int, lista_operadores: List[Tuple[str,int]]):
        """Configura el coordinador y arranca el health checker periódico."""
        self.host = host
        self.puerto = puerto
        self.trabajadores: List[InfoTrabajador] = [
            InfoTrabajador(f"operador-{i+1}", h, p) for i, (h, p) in enumerate(lista_operadores)
        ]
        # Timings que definimos
        self.intervalo_salud_seg = 3.0   # cada 3s se lanza un health-check
        self.timeout_salud_seg   = 3.0   # se espera hasta 3s la respuesta de cada ping
        self.timeout_calculo_seg = 4.0   # tiempo máximo por sub-tarea antes de que lo declare muerto jdsajdas

        self.hilo_salud = Repetidor(self.intervalo_salud_seg, self.verificar_salud, nombre="HealthChecker")
        self.socket_servidor = abrir_servidor(self.host, self.puerto)
        print(f"[coord] Escuchando clientes en {self.host}:{self.puerto}")
        self.hilo_salud.start()

    def _ahora(self) -> str:
        """Devuelve hora local HH:MM:SS para prefijar logs."""
        return datetime.datetime.now().strftime("%H:%M:%S")

    def verificar_salud(self):
        """Hace ping a cada operador; marca su estado y lo imprime en consola."""
        for operador in self.trabajadores:
            try:
                with abrir_cliente(operador.host, operador.puerto, timeout=self.timeout_salud_seg) as sock:
                    enviar_json(sock, {"type": "health"})
                    respuesta = recibir_json(sock, timeout=self.timeout_salud_seg)
                    ok = (respuesta.get("type") == "health_ok")
                with operador.lock:
                    operador.vivo = ok
                    if ok:
                        operador.ultimo_ok = time.time()
                estado = "OK" if ok else "NO-RESPONDE"
                print(f"[{self._ahora()}][salud] {operador.nombre} {estado}")
            except Exception:
                with operador.lock:
                    operador.vivo = False
                print(f"[{self._ahora()}][salud] {operador.nombre} DOWN")

    def trabajadores_vivos(self) -> List[InfoTrabajador]:
        """Retorna la lista de operadores actualmente marcados como vivos."""
        vivos = []
        for op in self.trabajadores:
            with op.lock:
                if op.vivo:
                    vivos.append(op)
        return vivos

    def enviar_subtarea(self, operador: InfoTrabajador, id_tarea: str, indice_parte: int,
                        subarreglo_izquierdo, subarreglo_derecho) -> Dict:
        """Envía un chunk de la suma al operador seleccionado y espera su 'result'."""
        with abrir_cliente(operador.host, operador.puerto, timeout=self.timeout_calculo_seg) as sock:
            enviar_json(sock, {"type": "compute_sum", "task_id": id_tarea, "idx": indice_parte,
                               "a": subarreglo_izquierdo, "b": subarreglo_derecho})
            return recibir_json(sock, timeout=self.timeout_calculo_seg)

    def calcular_suma_distribuida(self, arreglo_numeros_izquierda: List[int],
                                  arreglo_numeros_derecha: List[int]) -> List[int]:
        """Divide los arreglos en 2 mitades, asigna a operadores (con reintentos) y recombina."""
        if len(arreglo_numeros_izquierda) != len(arreglo_numeros_derecha):
            raise ValueError("Los arreglos deben tener la misma longitud")
        tam_total = len(arreglo_numeros_izquierda)
        mitad = tam_total // 2
        partes = [
            (0, arreglo_numeros_izquierda[:mitad], arreglo_numeros_derecha[:mitad]),
            (1, arreglo_numeros_izquierda[mitad:], arreglo_numeros_derecha[mitad:])
        ]
        id_tarea = str(uuid.uuid4())
        print(f"[coord] Nueva tarea {id_tarea} con n={tam_total} (chunks={len(partes)})")

        # --- imprimir los chunks a resolver ---
        for idx, a_chunk, b_chunk in partes:
            print(f"[coord] Tarea {id_tarea} → chunk {idx}: A={a_chunk}  B={b_chunk}")

        resultados_por_parte: Dict[int, List[int]] = {}

        def intentar_asignar(indice_parte: int, preferido: Optional[InfoTrabajador] = None) -> bool:
            """Intenta una asignación al operador preferido y, si falla, a cualquiera vivo."""
            sub_izq, sub_der = partes[indice_parte][1], partes[indice_parte][2]
            candidatos = self.trabajadores_vivos()
            if preferido and preferido in candidatos:
                candidatos.remove(preferido)
                candidatos.insert(0, preferido)
            for op in candidatos:
                try:
                    print(f"[coord] Enviando chunk {indice_parte} a {op.nombre}...")
                    respuesta = self.enviar_subtarea(op, id_tarea, indice_parte, sub_izq, sub_der)
                    if respuesta.get("type") == "result" and respuesta.get("idx") == indice_parte:
                        resultados_por_parte[indice_parte] = respuesta["result"]
                        print(f"[coord] Chunk {indice_parte} resuelto por {op.nombre} -> {respuesta['result']}")
                        return True
                    else:
                        print(f"[coord] Respuesta inesperada de {op.nombre}: {respuesta}")
                except Exception as exc:
                    print(f"[coord] {op.nombre} falló en chunk {indice_parte}: {exc}")
            return False

        vivos = self.trabajadores_vivos()
        if not vivos:
            self.verificar_salud()
            vivos = self.trabajadores_vivos()
        if not vivos:
            raise RuntimeError("No hay operadores vivos para procesar la solicitud")

        preferidos = {0: vivos[0], 1: (vivos[1] if len(vivos) > 1 else vivos[0])}

        for indice_parte, _, _ in partes:
            exito = intentar_asignar(indice_parte, preferidos[indice_parte])
            if not exito:
                print(f"[coord] Reintentando chunk {indice_parte} con cualquier operador vivo...")
                exito = intentar_asignar(indice_parte, None)
                if not exito:
                    raise RuntimeError(
                        f"No fue posible completar el chunk {indice_parte}; hay operadores caídos o sin respuesta."
                    )

        resultado_final = resultados_por_parte[0] + resultados_por_parte[1]
        # --- imprimir solución final ---
        print(f"[coord] Tarea {id_tarea} RESUELTA → resultado final = {resultado_final}")
        return resultado_final

    def atender_cliente(self, conexion: socket.socket, direccion):
        """Atiende una solicitud del cliente ('sum_arrays' o 'health') y responde."""
        try:
            solicitud = recibir_json(conexion, timeout=15.0)
            if solicitud.get("type") == "sum_arrays":
                arreglo_izq = solicitud["a"]
                arreglo_der = solicitud["b"]
                inicio = time.time()
                resultado_total = self.calcular_suma_distribuida(arreglo_izq, arreglo_der)
                duracion = time.time() - inicio
                enviar_json(
                    conexion,
                    {"type": "ok", "result": resultado_total, "elapsed": duracion,
                     "operadores": [
                         {"nombre": op.nombre, "host": op.host, "puerto": op.puerto, "vivo": op.vivo}
                         for op in self.trabajadores]}
                )
            elif solicitud.get("type") == "health":
                enviar_json(conexion, {"type": "health_ok", "role": "coordinator"})
            else:
                enviar_json(conexion, {"type": "error", "error": "unknown_request"})
        except Exception as e:
            try:
                enviar_json(conexion, {"type": "error", "error": str(e)})
            except Exception:
                pass
        finally:
            conexion.close()

    def servir(self):
        """Bucle del servidor de cálculo: acepta conexiones de clientes y las delega a hilos."""
        print("[coord] Servidor de cálculo iniciado")
        while True:
            conexion, direccion = self.socket_servidor.accept()
            threading.Thread(target=self.atender_cliente, args=(conexion, direccion), daemon=True).start()

if __name__ == "__main__":
    """Punto de entrada: parsea flags y lanza el coordinador (servidor de cálculo)."""
    ap = argparse.ArgumentParser(description="Servidor de Cálculo (Coordinador)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--operadores", nargs="+",
                    default=["127.0.0.1:6001", "127.0.0.1:6002"], help="host:port ...")
    args = ap.parse_args()

    tuplas_operadores = []
    for w in args.operadores:
        h, p = w.split(":")
        tuplas_operadores.append((h, int(p)))

    cont = Counter(tuplas_operadores)
    duplicados = [f"{h}:{p}" for (h, p), c in cont.items() if c > 1]
    if duplicados:
        print(f"[coord] Error: --uno de los operadores tiene mal asignada la ip y el puerto (repetidos): {', '.join(duplicados)}")
        sys.exit(1)

    Coordinador(args.host, args.port, tuplas_operadores).servir()
