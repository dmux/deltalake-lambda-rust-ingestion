# Lambda container image for provided.al2023 runtime (ARM64)
# The bootstrap binary is copied from the local cross-compiled release build.
# Run `make local-image-deploy` to build, push to MiniStack ECR, and deploy.

FROM public.ecr.aws/lambda/provided:al2023-arm64

# provided.al2023 runtime looks for the handler at /var/runtime/bootstrap
COPY target/lambda/bootstrap/bootstrap /var/runtime/bootstrap
RUN chmod +x /var/runtime/bootstrap

CMD ["bootstrap"]
