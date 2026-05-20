import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = 'video_id'

            cur.execute(
                f"""INSERT INTON{schema}.{table}("video_ID", "video_title", "upload_date", "duration", "video_views", "likes_count", "comments_count")
                VALUES (%(video_id)s, %(title), %(publishedAt), %(duration)s, %(viewCount)s, %(likeCounts)s, %(CommentCounts)s)
                """, row
            )
        else:
            video_id = 'Video_ID'

            cur.execute(
                f"""INSERT INTON{schema}.{table}("video_ID", "video_title", "upload_date", "duration", "video_types", "video_views", "likes_count", "comments_count")
                VALUES (%(video_id)s, %(title), %(publishedAt), %(duration)s, %(viewCount)s, %(likeCounts)s, %(CommentCounts)s)
                """, row
            )
        
        conn.commit()
        logger.info(f"Inserted row with video_ID: {row[video_id]}")
    
    except Exception as e:
        logger.error(f"Error inserting row with video_ID: {row[video_id]}")
        raise e
    

def update_rows(cur, conn, schema, row):
    
    try:
        #staging
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = "commentCount"
        #core
        else:
            video_id = 'video_id'
            upload_date = 'Upload_Date'
            video_title = 'title'
            video_views = 'video_views'
            likes_count = 'Likes_Count'
            comments_count = "Comments_Count"

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """, row
        )

        conn.commit()

        logger.info(f"Updated row with Video_ID: {row[video_id]}")
    

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e
    

    
def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            Where "Video_ID" IN {ids_to_delete};
            """
        )

        conn.commit()
        logger.info(f"Error deleting rows with Video_IDs: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Erro deleting rows with Video_IDS: {ids_to_delete} - {e}")
        raise e