"""
exclude_api.py
FastAPI router that manages excluded sellers and Canon-specific excluded keywords
stored in SQLite via ``db.exclusions_db``. Provides clear error messages and Pydantic
validation.

Mount this router in your FastAPI app with a prefix, e.g.:
    app.include_router(router, prefix="/exclude", tags=["Exclude Management"])

Endpoints:
    Sellers:
        GET    /sellers
        POST   /sellers                {"name": "<seller>"}
        DELETE /sellers/{name}
        PUT    /sellers/{old_name}     {"name": "<new_name>"}

    Canon Keywords:
        GET    /canon/keywords
        POST   /canon/keywords         {"phrase": "<keyword/phrase>"}
        DELETE /canon/keywords/{phrase}
        PUT    /canon/keywords/{old_phrase}  {"phrase": "<new_phrase>"}

    Xerox Keywords:
        GET    /xerox/keywords
        POST   /xerox/keywords         {"phrase": "<keyword/phrase>"}
        DELETE /xerox/keywords/{phrase}
        PUT    /xerox/keywords/{old_phrase}  {"phrase": "<new_phrase>"}
"""

from __future__ import annotations


from typing import List

from fastapi import APIRouter, FastAPI, HTTPException, status
from pydantic import BaseModel, Field


from db.exclusions_db import (
    list_sellers as db_list_sellers,
    add_seller as db_add_seller,
    remove_seller as db_remove_seller,
    list_canon_keywords as db_list_canon_keywords,
    add_canon_keyword as db_add_canon_keyword,
    remove_canon_keyword as db_remove_canon_keyword,
    list_xerox_keywords as db_list_xerox_keywords,
    add_xerox_keyword as db_add_xerox_keyword,
    remove_xerox_keyword as db_remove_xerox_keyword,
)


router = APIRouter()

# ---- Models ----

class SellerIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=256)

class KeywordIn(BaseModel):
    phrase: str = Field(..., min_length=1, max_length=256)

class MessageOut(BaseModel):
    message: str



# ---- DB-based helpers ----
def _norm(s: str) -> str:
    return " ".join(s.strip().split())

def _update_seller(old_name: str, new_name: str) -> MessageOut:
    old_key = _norm(old_name)
    new_key = _norm(new_name)
    if not new_key:
        raise HTTPException(status_code=400, detail="New value cannot be empty")
    sellers = db_list_sellers()
    if old_key not in sellers:
        raise HTTPException(status_code=404, detail="Seller not found")
    if new_key != old_key and new_key in sellers:
        raise HTTPException(status_code=400, detail="Seller already exists")
    db_remove_seller(old_key)
    db_add_seller(new_key)
    return MessageOut(message="Updated")

def _update_keyword(old_phrase: str, new_phrase: str) -> MessageOut:
    old_key = _norm(old_phrase)
    new_key = _norm(new_phrase)
    if not new_key:
        raise HTTPException(status_code=400, detail="New value cannot be empty")
    keywords = db_list_canon_keywords()
    if old_key not in keywords:
        raise HTTPException(status_code=404, detail="Keyword not found")
    if new_key != old_key and new_key in keywords:
        raise HTTPException(status_code=400, detail="Keyword already exists")
    db_remove_canon_keyword(old_key)
    db_add_canon_keyword(new_key)
    return MessageOut(message="Updated")

def _update_xerox_keyword(old_phrase: str, new_phrase: str) -> MessageOut:
    old_key = _norm(old_phrase)
    new_key = _norm(new_phrase)
    if not new_key:
        raise HTTPException(status_code=400, detail="New value cannot be empty")
    keywords = db_list_xerox_keywords()
    if old_key not in keywords:
        raise HTTPException(status_code=404, detail="Keyword not found")
    if new_key != old_key and new_key in keywords:
        raise HTTPException(status_code=400, detail="Keyword already exists")
    db_remove_xerox_keyword(old_key)
    db_add_xerox_keyword(new_key)
    return MessageOut(message="Updated")


# ---- Sellers Routes ----


@router.get("/sellers", response_model=List[str], summary="List all excluded sellers")
def list_sellers() -> List[str]:
    return db_list_sellers()



@router.post(
    "/sellers",
    response_model=MessageOut,
    status_code=status.HTTP_201_CREATED,
    summary="Add a seller to the exclude list",
)
def add_seller(seller: SellerIn) -> MessageOut:
    key = _norm(seller.name)
    if not key:
        raise HTTPException(status_code=400, detail="Value cannot be empty")
    sellers = db_list_sellers()
    if key in sellers:
        raise HTTPException(status_code=400, detail="Seller already excluded")
    db_add_seller(key)
    return MessageOut(message="Added")



@router.delete(
    "/sellers/{name}",
    response_model=MessageOut,
    summary="Remove a seller from the exclude list",
)
def remove_seller(name: str) -> MessageOut:
    key = _norm(name)
    sellers = db_list_sellers()
    if key not in sellers:
        raise HTTPException(status_code=404, detail="Seller not found")
    db_remove_seller(key)
    return MessageOut(message="Removed")



@router.put(
    "/sellers/{old_name}",
    response_model=MessageOut,
    summary="Rename an excluded seller",
)
def update_seller(old_name: str, seller: SellerIn) -> MessageOut:
    return _update_seller(old_name, seller.name)



# ---- Keywords Routes ----

@router.get(
    "/canon/keywords",
    response_model=List[str],
    summary="List Canon-specific excluded keywords/phrases",
)
def list_canon_keywords() -> List[str]:
    return db_list_canon_keywords()

@router.post(
    "/canon/keywords",
    response_model=MessageOut,
    status_code=status.HTTP_201_CREATED,
    summary="Add a Canon keyword/phrase to the exclude list",
)
def add_canon_keyword(keyword: KeywordIn) -> MessageOut:
    key = _norm(keyword.phrase)
    if not key:
        raise HTTPException(status_code=400, detail="Value cannot be empty")
    keywords = db_list_canon_keywords()
    if key in keywords:
        raise HTTPException(status_code=400, detail="Keyword already excluded")
    db_add_canon_keyword(key)
    return MessageOut(message="Added")

@router.delete(
    "/canon/keywords/{phrase}",
    response_model=MessageOut,
    summary="Remove a Canon keyword/phrase from the exclude list",
)
def remove_canon_keyword(phrase: str) -> MessageOut:
    key = _norm(phrase)
    keywords = db_list_canon_keywords()
    if key not in keywords:
        raise HTTPException(status_code=404, detail="Keyword not found")
    db_remove_canon_keyword(key)
    return MessageOut(message="Removed")

@router.put(
    "/canon/keywords/{old_phrase}",
    response_model=MessageOut,
    summary="Rename a Canon excluded keyword/phrase",
)
def update_canon_keyword(old_phrase: str, keyword: KeywordIn) -> MessageOut:
    return _update_keyword(old_phrase, keyword.phrase)


# ---- Xerox Keywords Routes ----

@router.get(
    "/xerox/keywords",
    response_model=List[str],
    summary="List Xerox-specific excluded keywords/phrases",
)
def list_xerox_keywords() -> List[str]:
    return db_list_xerox_keywords()

@router.post(
    "/xerox/keywords",
    response_model=MessageOut,
    status_code=status.HTTP_201_CREATED,
    summary="Add a Xerox keyword/phrase to the exclude list",
)
def add_xerox_keyword(keyword: KeywordIn) -> MessageOut:
    key = _norm(keyword.phrase)
    if not key:
        raise HTTPException(status_code=400, detail="Value cannot be empty")
    keywords = db_list_xerox_keywords()
    if key in keywords:
        raise HTTPException(status_code=400, detail="Keyword already excluded")
    db_add_xerox_keyword(key)
    return MessageOut(message="Added")

@router.delete(
    "/xerox/keywords/{phrase}",
    response_model=MessageOut,
    summary="Remove a Xerox keyword/phrase from the exclude list",
)
def remove_xerox_keyword(phrase: str) -> MessageOut:
    key = _norm(phrase)
    keywords = db_list_xerox_keywords()
    if key not in keywords:
        raise HTTPException(status_code=404, detail="Keyword not found")
    db_remove_xerox_keyword(key)
    return MessageOut(message="Removed")

@router.put(
    "/xerox/keywords/{old_phrase}",
    response_model=MessageOut,
    summary="Rename a Xerox excluded keyword/phrase",
)
def update_xerox_keyword(old_phrase: str, keyword: KeywordIn) -> MessageOut:
    return _update_xerox_keyword(old_phrase, keyword.phrase)


