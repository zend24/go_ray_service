FROM rayproject/ray-ml:2.7.0

COPY basic-ray.py /app/basic-ray.py

RUN pip install ray[serve]

CMD ["python", "/app/basic-ray.py"]