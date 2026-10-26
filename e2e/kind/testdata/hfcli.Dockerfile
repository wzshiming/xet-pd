# Pinned like the hf CLI of the test job in .github/workflows/ci.yml.
FROM python:3.13-slim
RUN pip install --no-cache-dir 'huggingface_hub[hf_xet]==2.2.0' 'hf_xet==1.7.0'
