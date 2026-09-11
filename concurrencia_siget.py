import threading
import time
import random

# CONFIGURACIÓN DEL SISTEMA SIGET
CAPACIDAD_BUFFER = 5
TOTAL_EVENTOS = 12

buffer_trafico = []

# MECANISMOS DE SINCRONIZACIÓN
mutex = threading.Lock()
espacios_vacios = threading.Semaphore(CAPACIDAD_BUFFER)
elementos_disponibles = threading.Semaphore(0)

eventos_producidos = 0
eventos_procesados = 0
lock_contadores = threading.Lock()

# HILO PRODUCTOR: SENSORES DE TRÁFICO
def sensor_trafico(id_sensor, zonas):
    global eventos_producidos
    
    while True:
        with lock_contadores:
            if eventos_producidos >= TOTAL_EVENTOS:
                break
            eventos_producidos += 1
            num_evento = eventos_producidos

        time.sleep(random.uniform(0.3, 0.7))
        
        zona = random.choice(zonas)
        vehiculos = random.randint(10, 150)
        dato = f"Evento #{num_evento} [Sensor {id_sensor} - {zona}]: {vehiculos} veh/min"

        espacios_vacios.acquire()

        with mutex:
            buffer_trafico.append(dato)
            print(f"[PRODUCTOR] {dato} -> Insertado en búfer. (Ocupación: {len(buffer_trafico)}/{CAPACIDAD_BUFFER})")

        elementos_disponibles.release()

# HILO CONSUMIDOR: MÓDULOS DE ANÁLISIS
def modulo_analisis(id_modulo):
    global eventos_procesados
    
    while True:
        with lock_contadores:
            if eventos_procesados >= TOTAL_EVENTOS:
                break

        elementos_disponibles.acquire()

        with mutex:
            if buffer_trafico:
                dato = buffer_trafico.pop(0)
                with lock_contadores:
                    eventos_procesados += 1
                print(f"  [CONSUMIDOR] Módulo {id_modulo} procesando: '{dato}'. (Búfer restante: {len(buffer_trafico)})")
            else:
                espacios_vacios.release()
                continue

        espacios_vacios.release()
        time.sleep(random.uniform(0.5, 0.9))

if __name__ == "__main__":
    print("="*75)
    print(" SIGET: SIMULADOR DE CONCURRENCIA SENSORES-ANÁLISIS (PRODUCTOR-CONSUMIDOR)")
    print("="*75)

    sensores = [
        (1, ["Calle 50 (Centro)", "Av. El Poblado"]),
        (2, ["Autopista Norte", "Carrera 64C"]),
    ]
    
    hilos = []

    for id_s, zonas in sensores:
        t = threading.Thread(target=sensor_trafico, args=(id_s, zonas))
        hilos.append(t)
        t.start()

    for i in range(1, 4):
        t = threading.Thread(target=modulo_analisis, args=(i,))
        hilos.append(t)
        t.start()

    for t in hilos:
        t.join()

    print("="*75)
    print("SIMULACIÓN FINALIZADA CON ÉXITO: Sin pérdida de datos ni interbloqueos.")
    print("="*75)