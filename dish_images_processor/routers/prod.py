from fastapi import APIRouter


prod_router = APIRouter()

@prod_router.post("/preprocess")
async def preprocess_img():
    pass
