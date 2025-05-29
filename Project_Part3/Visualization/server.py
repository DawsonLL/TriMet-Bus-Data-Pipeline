import os
from http.server import SimpleHTTPRequestHandler, HTTPServer

# Run program: "python server.py"
# this will ensure program properly adds the environment variable to the index.html file
PORT = 8000
API_KEY = os.getenv("MAPBOX_API_KEY", "")

class CustomHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            # Read the original index.html
            with open("index.html", "r") as f:
                content = f.read()
            # Replace a placeholder with the API key
            content = content.replace("{{MAPBOX_API_KEY}}", API_KEY)
            
            # Send response headers
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            
            # Send the modified content
            self.wfile.write(content.encode("utf-8"))
        else:
            # Serve other files normally
            super().do_GET()

if __name__ == "__main__":
    httpd = HTTPServer(("", PORT), CustomHandler)
    print(f"Serving on port {PORT}")
    httpd.serve_forever()

