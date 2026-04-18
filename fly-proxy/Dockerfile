FROM denoland/deno:2.1.4

WORKDIR /app
COPY main.ts .

RUN deno cache main.ts

EXPOSE 8080

CMD ["run", "--allow-net", "--allow-env", "main.ts"]
