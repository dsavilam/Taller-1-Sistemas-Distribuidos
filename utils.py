import json, socket, threading, errno
from typing import Any, Dict

CODIFICACION = "utf-8"

def enviar_json(sock: socket.socket, obj: Dict[str, Any]) -> None:
    """Envía un diccionario como una línea JSON terminada en '\n' por el socket.
    Parámetros:
      - sock: socket conectado en modo stream (TCP).
      - obj: diccionario serializable a JSON.
    No retorna valor; lanza excepción si la conexión falla.
    """
    data = (json.dumps(obj, separators=(",", ":")) + "\n").encode(CODIFICACION)
    sock.sendall(data)

def recibir_json(sock: socket.socket, timeout: float = None) -> Dict[str, Any]:
    """Recibe una línea JSON (hasta '\n') desde el socket y la decodifica a dict.
    Parámetros:
      - sock: socket conectado en modo stream (TCP).
      - timeout: segundos máximos para esperar datos (None = sin cambio).
    Retorna:
      - dict con el contenido decodificado.
    Lanza:
      - ConnectionError si el peer cerró; socket.timeout si expira.
    """
    if timeout is not None:
        sock.settimeout(timeout)
    buf = bytearray()
    while True:
        trozo = sock.recv(4096)
        if not trozo:
            raise ConnectionError("Socket cerrado por el par")
        buf.extend(trozo)
        nl = buf.find(b"\n")
        if nl != -1:
            linea = buf[:nl].decode(CODIFICACION)
            return json.loads(linea)

def abrir_cliente(host: str, puerto: int, timeout: float = 5.0) -> socket.socket:
    """Abre un socket TCP cliente y conecta al host:puerto con timeout.
    Retorna el socket conectado listo para usar."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, puerto))
    return s

def abrir_servidor(host: str, puerto: int, backlog: int = 100) -> socket.socket:
    """Crea un socket TCP servidor en host:puerto y comienza a escuchar."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Verificamos que no usen la misma ip y el mismo puerto
    # El hilo que explica esto es medio largo, pero leanlo: https://github.com/python-trio/trio/issues/928
    # Esto solo en Windows ya que en Linux SO_REUSEADDR si permite el dual binding a una ip en 
    # el mismo puerto si uno de los dos sockets esta en TIME_WAIT

    # If two sockets try to bind to the same port and IP, 
    # BSDs will only allow it if one of them is in TIME_WAIT state, and the new socket has SO_REUSEADDR set. 
    # This is useful for restarting servers quickly after shutdown."""

    # Documentación oficial: https://learn.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse
    if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
        try:
            # En windows
            s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        except OSError:
            pass
    else:
        # En el caso de linux
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind((host, puerto))
    except OSError as e:
        en = getattr(e, "errno", None)
        we = getattr(e, "winerror", None)  # 10048 en windows = address in use
        s.close()
        if en == errno.EADDRINUSE or we == 10048:
            raise RuntimeError(f"Puerto {host}:{puerto} ocupado por otro proceso (Otro operador esta escuchando o tienes el puerto ocupado).")
        raise
    s.listen(backlog)
    return s

class Repetidor(threading.Thread):
    """Hilo que ejecuta periódicamente la función dada cada 'intervalo' segundos."""
    def __init__(self, intervalo: float, funcion, nombre: str = "Repetidor", daemon: bool = True):
        """Configura el repetidor.
        - intervalo: seg entre ejecuciones
        - funcion: callable sin argumentos a ejecutar
        - nombre: nombre del hilo
        - daemon: si el hilo es daemon
        """
        super().__init__(daemon=daemon, name=nombre)
        self.intervalo = intervalo
        self.funcion = funcion
        self._parar = threading.Event()

    def detener(self):
        """Solicita detener el ciclo periódico."""
        self._parar.set()

    def run(self):
        """Bucle principal que ejecuta la función y espera 'intervalo' seg."""
        while not self._parar.is_set():
            try:
                self.funcion()
            except Exception as e:
                print(f"[{self.name}] Error: {e}")
            finally:
                self._parar.wait(self.intervalo)
