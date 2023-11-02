FROM nginx:1.23.4
RUN rm /etc/nginx/conf.d/default.conf
COPY default.conf /etc/nginx/conf.d/
CMD ["nginx", "-g", "daemon off;"]
