from django.core.management.base import BaseCommand
from django.conf import settings
import pika
import json
import os
import sys

class Command(BaseCommand):
    help = "Subscribe to RabbitMQ and print alert messages to the Django console."

    def handle(self, *args, **options):
        # Leer configuración desde settings o variables de entorno (fallback a tus valores)
        rabbit_host = getattr(settings, "RABBIT_HOST", os.environ.get("RABBIT_HOST", "host"))
        rabbit_user = getattr(settings, "RABBIT_USER", os.environ.get("RABBIT_USER", "monitoring_user"))
        rabbit_password = getattr(settings, "RABBIT_PASSWORD", os.environ.get("RABBIT_PASSWORD", "isis2503"))
        exchange = getattr(settings, "RABBIT_EXCHANGE", os.environ.get("RABBIT_EXCHANGE", "monitoring_measurements"))
        topics = getattr(settings, "RABBIT_TOPICS", os.environ.get("RABBIT_TOPICS", "ML.505.#")).split(",")  # coma-sep opcional

        # Umbral de alarma configurable (por defecto 30.0)
        try:
            threshold = float(os.environ.get("ALARM_THRESHOLD", getattr(settings, "ALARM_THRESHOLD", 30.0)))
        except Exception:
            threshold = 30.0

        # Conexión a RabbitMQ
        credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
        params = pika.ConnectionParameters(host=rabbit_host, credentials=credentials)
        try:
            connection = pika.BlockingConnection(params)
        except Exception as e:
            self.stderr.write(f"Error conectando a RabbitMQ: {e}")
            sys.exit(1)

        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type='topic')

        # Cola efímera
        result = channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue

        for topic in topics:
            topic = topic.strip()
            if topic:
                channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=topic)

        self.stdout.write("> Esperando mediciones. Para salir presiona CTRL+C")
        self.stdout.write(f"> Umbral de alarma: {threshold}")

        def callback(ch, method, properties, body):
            # Parseo defensivo del payload
            try:
                text = body.decode('utf8')
                # tu publisher usa comillas simples: {'value':10.1,'unit':'C'}
                payload = json.loads(text.replace("'", '"'))
            except Exception as e:
                self.stderr.write(f"Payload no válido: {body!r} - {e}")
                return

            # Obtener valor y unidad
            value = payload.get('value')
            unit = payload.get('unit', '')

            # intento de conversión a número
            try:
                value_num = float(value)
            except Exception:
                # si no se puede convertir, lo mostramos pero no evaluamos
                self.stdout.write(f"Medición recibida (no numérica): {payload!r} - routing_key={method.routing_key}")
                return

            # Extraer tema para identificar variable (ej. ML.505.Temperature)
            routing = method.routing_key or ""
            parts = routing.split('.')
            variable_name = parts[2] if len(parts) > 2 else None

            # Solo interesa Temperature (según tu ejemplo)
            if variable_name and variable_name.lower() == 'temperature':
                if value_num > threshold:
                    # Mensaje de alerta en la consola de Django (resaltado como ERROR)
                    self.stderr.write(f"ALERTA: Temperatura alta recibida -> {value_num}{unit} (routing_key={routing})")
                else:
                    # Mensaje informativo (puedes comentarlo si solo quieres alertas)
                    self.stdout.write(f"OK: Temperatura {value_num}{unit} (routing_key={routing})")
            else:
                # Si no es Temperature, solo lo mostramos como info (opcional)
                self.stdout.write(f"Medición recibida (no Temperature): {payload!r} (routing_key={routing})")

        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.stdout.write("\nInterrumpido por usuario, cerrando conexión...")
            try:
                connection.close()
            except Exception:
                pass
