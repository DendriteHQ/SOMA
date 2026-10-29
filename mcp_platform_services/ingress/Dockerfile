FROM nginx:1.28-alpine
COPY mcp_platform_services/ingress/nginx.conf /etc/nginx/nginx.conf
EXPOSE 8000 8050
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8000/health/live || exit 1
