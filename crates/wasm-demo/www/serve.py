#!/usr/bin/env python3
"""Simple HTTP server with cross-origin isolation headers for SharedArrayBuffer support."""

import http.server
import socketserver

PORT = 8080

class COIHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # Required for SharedArrayBuffer (wasm-bindgen-rayon)
        self.send_header('Cross-Origin-Opener-Policy', 'same-origin')
        self.send_header('Cross-Origin-Embedder-Policy', 'require-corp')
        super().end_headers()

with socketserver.TCPServer(("", PORT), COIHandler) as httpd:
    print(f"Serving at http://localhost:{PORT}")
    print("Cross-origin isolation enabled for SharedArrayBuffer support")
    httpd.serve_forever()
