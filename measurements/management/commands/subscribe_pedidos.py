from django.core.management.base import BaseCommand
from django.conf import settings
import pika
import json
import os
import sys

class Command(BaseCommand):
    help = "Subscribe to RabbitMQ and always print an alert message to the Django console for every received message."

    def handle(self, *args, **options):
        # Configuración: lee de settings o variables de entorno (fallback a tus valores)
        rabbit_host = getattr(settings, "RABBIT_HOST", os.environ.get("RABBIT_HOST", "172.31.22.207"))
        rabbit_user = getattr(settings, "RABBIT_USER", os.environ.get("RABBIT_USER", "monitoring_user"))
        rabbit_password = getattr(settings, "RABBIT_PASSWORD", os.environ.get("RABBIT_PASSWORD", "isis2503"))
        exchange = getattr(settings, "RABBIT_EXCHANGE", os.environ.get("RABBIT_EXCHANGE", "monitoring_measurements"))
        topics = getattr(settings, "RABBIT_TOPICS", os.environ.get("RABBIT_TOPICS", "ML.505.#")).split(",")

        credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
        params = pika.ConnectionParameters(host=rabbit_host, credentials=credentials)
        try:
            connection = pika.BlockingConnection(params)
        except Exception as e:
            self.stderr.write(f"Error conectando a RabbitMQ: {e}")
            sys.exit(1)

        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type='topic')

        result = channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue

        for topic in topics:
            t = topic.strip()
            if t:
                channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=t)

        self.stdout.write("> Esperando mediciones. Para salir presiona CTRL+C")

        def callback(ch, method, properties, body):
            # Parseo defensivo del payload (el publisher usa comillas simples)
            try:
                text = body.decode('utf8')
                payload = json.loads(text.replace("'", '"'))
            except Exception as e:
                payload = {"raw": body.decode('utf8', errors='replace')}
                self.stderr.write(f"Payload no parseable: {e}")

            routing = method.routing_key if method and method.routing_key else "(no routing_key)"
            # Siempre imprimir un mensaje de alerta cuando llegue cualquier mensaje
            # Formato: ALERTA: <routing_key> - <payload>
            try:
                self.stdout.write(f"ALERTA: mensaje recibido -> routing_key={routing} payload={payload}")
            except Exception:
                # fallback si payload no serializable
                self.stdout.write(f"ALERTA: mensaje recibido -> routing_key={routing} payload (no serializable)")

        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.stdout.write("\nInterrumpido por usuario, cerrando conexión...")
            try:
                connection.close()
            except Exception:
                pass

