import re
import logging
from fastapi import FastAPI
from src.backend import OptiSkane
from pydantic import BaseModel, model_validator


app = FastAPI()


# Models
class SearchRequest(BaseModel):
    origin: tuple[float, float]
    destination: tuple[float, float]
    departure_time: str | None = None

    @model_validator(mode="after")
    def validate_attrs(self):
        # Checking latitudes
        if abs(self.origin[0]) > 90 or abs(self.destination[0]) > 90:
            raise Exception("Wrong latitudes.")

        # Checking longitudes
        if abs(self.origin[1]) > 180 or abs(self.destination[1]) > 180:
            raise Exception("Wrong longitudes.")

        # Checking departure_time
        pattern = r"\d{2}:\d{2}:\d{2}"
        if self.departure_time is not None and not re.match(pattern, self.departure_time):
            raise Exception("Wrong departure_time format.")

        return self

    def __repr__(self):
        return f"<Origin: {self.origin} -> Destination: {self.destination} Departure time: ({self.departure_time})>"


# Endpoints
@app.post("/search")
def search(request: SearchRequest):
    return backend.queue(request)


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] - %(message)s",
        datefmt="%H:%M:%S"
    )

    # Setup backend
    backend = OptiSkane()

    # Setup app
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
