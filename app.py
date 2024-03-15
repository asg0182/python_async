import sys
import random
import uuid
import logging
from flask import Flask, request, jsonify, make_response


logger = logging.getLogger(name="SERVER")
formatter = logging.Formatter("%(asctime)s %(name)s: %(levelname)s - %(message)s")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


app = Flask(__name__)
app.config["jobs"] = {
    "build": {},
    "git-working": {},
    "deploy": {},
    "start": {},
    "stop": {},
    "restart": {}
}
app.config["requests"] = {}


@app.route("/jobs/<job_id>", methods=["GET", "POST"])
def start_job(job_id):
    jobs = app.config["jobs"]
    reqs = app.config["requests"]
    if request.method == "POST":
        if job_id not in jobs:
            return make_response(
                jsonify({"error": "Job doesnt exists"}),
                400
            )
        req_id = str(uuid.uuid4())
        reqs[req_id] = {
            "id": req_id,
            "job_id": job_id,
            "count": random.randint(10, 30),
            "status": "running"
        }

        logger.info(f"{job_id} with request {req_id} is started")
        return make_response(
            jsonify({"id": req_id}),
            201
        )
    elif request.method == "GET":
        ids = []
        for req_id, req_data in reqs.items():
            if req_data["job_id"] == job_id:
                ids.append(req_data)
        return make_response(
            jsonify({"job_id": job_id, "requests": ids}),
            200
        )


@app.route("/requests/<request_id>", methods=["GET"])
def request_status(request_id):
    reqs = app.config["requests"]
    req = reqs.get(request_id)
    logger.info(f"{request_id} ---- {reqs}")
    if not req:
        return make_response(jsonify({"error": "Not found"}), 404)

    if req["count"] <= 0:
        req["status"] = "success"
        req = reqs.pop(request_id)
        return make_response(jsonify(req), 200)

    req["count"] -= 1
    return make_response(jsonify(req), 200)


if __name__ == "__main__":
    app.run()
