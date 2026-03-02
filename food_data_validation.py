from pydantic import BaseModel
from typing import Dict, List, Optional, Any


class FoodItem(BaseModel):
    ## food items validation
    food_id: Optional[str] = None
    food_name: Optional[str] = None
    price: Optional[float] = None
    image_url: Optional[str] = None
    description: Optional[str] = None

class RestaurantDetail(BaseModel):
    ## Restaurant detail validation
    restaurant_id : Optional[str] = None
    restaurant_name : Optional[str] = None
    cuisine : Optional[str] = None
    rating : Optional[float] = None
    restaurant_image : Optional[str] = None
    opening_time : Dict[str, Any] = {}
    distance : Optional[str] = None
    menu_detail : Dict[str, List[FoodItem]]









