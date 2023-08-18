import http.server
import socketserver
from pathlib import Path


def serve_folder(folder: Path):
    class RenderRequestHandler(http.server.SimpleHTTPRequestHandler):
        def translate_path(self, path: str):
            return str(folder / path.lstrip("/"))

    PORT = 4000

    with socketserver.TCPServer(("0.0.0.0", PORT), RenderRequestHandler) as httpd:
        print(f"Serving at port http://0.0.0.0:{PORT}")
        httpd.serve_forever()
