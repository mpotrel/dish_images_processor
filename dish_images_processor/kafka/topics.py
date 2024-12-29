# TODO: Make this a yaml file so it can be changed without deployments
TOPICS = {
    "background_removal": {
        "input": "background-removal-input",
        "output": "background-generation-input"
    },
    "background_generation": {
        "input": "background-generation-input",
        "output": "hyper-resolution-input"
    },
    "hyper_resolution": {
        "input": "hyper-resolution-input",
        "output": "completed-images"
    }
}
