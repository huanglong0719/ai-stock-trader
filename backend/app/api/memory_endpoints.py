from fastapi import APIRouter, UploadFile, File, HTTPException, Response
from pydantic import BaseModel
from app.services.learning_service import learning_service
from datetime import datetime

router = APIRouter()

@router.get("/memory/export")
async def export_memory(format: str = "json"):
    """导出记忆数据"""
    try:
        content = await learning_service.export_all_memories(format)
        media_type = "application/json" if format == "json" else "text/csv"
        filename = f"memory_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format}"
        
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/memory/import")
async def import_memory(file: UploadFile = File(...)):
    """导入记忆数据"""
    try:
        content = await file.read()
        content_str = content.decode("utf-8")
        
        # 简单判断格式
        if (file.filename or "").endswith(".csv"):
            fmt = "csv"
        else:
            fmt = "json"
            
        result = await learning_service.import_memories(content_str, fmt)
        return {"status": "success", "details": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
