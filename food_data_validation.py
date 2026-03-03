from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any


class MenuItem(BaseModel):
    ## food items validation
    restaurant_id: str
    category_name: str
    food_id: Optional[str] = None
    food_name: Optional[str] = None
    price: Optional[float] = None
    image_url: Optional[str] = None
    description: Optional[str] = None

class RestaurantDetail(BaseModel):
    ## Restaurant detail validation
    restaurant_id : str
    restaurant_name : Optional[str] = None
    cuisine : Optional[str] = None
    rating : Optional[float] = None
    restaurant_image : Optional[str] = None
    opening_time : Dict[str, Any] = {}
    distance : Optional[str] = None


class RestaurantData(BaseModel):
    restaurant_detail: RestaurantDetail
    Menu_Items: List[MenuItem] = Field(default_factory=list)









