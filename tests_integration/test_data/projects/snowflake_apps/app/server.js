const http = require("http");

http
  .createServer((_request, response) => {
    response.writeHead(200, { "Content-Type": "text/plain" });
    response.end("ok");
  })
  .listen(3000, "0.0.0.0");
