import sqlmodel
from .pixiv_sqlmodel import *
import random


class PixivDB:
    def __init__(self, db_url: str):
        self.engine = sqlmodel.create_engine(db_url)
        sqlmodel.SQLModel.metadata.create_all(self.engine)
        self.session = sqlmodel.Session(self.engine)
        # 启用外键约束
        self.session.connection().execute(sqlmodel.text("PRAGMA foreign_keys=ON"))

    def insert_daily_illust_source_rows(self, rows: list[DailyIllustSource]):
        for row in rows:
            self.session.merge(row)
        self.session.commit()

    def get_daily_illust_nums(self):
        statement = sqlmodel.select(sqlmodel.func.count()).select_from(DailyIllustSource)
        return self.session.exec(statement).first()

    def get_random_daily_illust(self, expire_time: datetime) -> DailyIllustSource | None:
        # 1. 定义统一的过滤条件，确保 Count 和 Select 使用完全相同的逻辑
        # 逻辑：用户有效 AND (从未发布过 OR 上次发布时间早于过期时间)
        # 注意：使用 sqlmodel.or_ 处理 "或" 逻辑
        where_conditions = [
            DailyIllustSource.user_id != 0,
            sqlmodel.or_(
                sqlmodel.col(DailyIllustSource.last_post).is_(None),  # 处理 NULL (从未发布)
                DailyIllustSource.last_post < expire_time  # 处理过期
            )
        ]
        # 2. 获取符合条件的数量
        cnt_statement = (
            sqlmodel.select(func.count())
            .where(*where_conditions)  # 解包传入所有条件
            .select_from(DailyIllustSource)
        )
        rows_cnt = self.session.exec(cnt_statement).first() or 0
        # 3. 如果没有符合条件的图片，直接返回
        if rows_cnt < 1:
            return None
        # 4. 计算随机偏移量 (即使 rows_cnt 为 1，randint(0, 0) 也是安全的，不需要单独写 elif)
        random_offset = random.randint(0, rows_cnt - 1)
        # 5. 取出具体的那一行
        # 关键修正：这里必须加上和上面一样的 .where(*where_conditions)，否则 offset 会偏离
        statement = (
            sqlmodel.select(DailyIllustSource)
            .where(*where_conditions)
            .offset(random_offset)
            .limit(1)
        )
        illust = self.session.exec(statement).first()
        # 双重保险，理论上上面 rows_cnt > 0 这里就不该为 None
        if illust is None:
            return None
        # 6. 更新时间 (原子操作)
        self.session.exec(
            sqlmodel.update(DailyIllustSource)
            .where(sqlmodel.col(DailyIllustSource.work_id) == illust.work_id)
            .values(last_post=func.now())
        )
        self.session.commit()
        return illust

    def get_daily_illust_source_row(self, work_id: int) -> DailyIllustSource | None:
        return self.session.get(DailyIllustSource, work_id)

    def shutdown(self):
        self.session.close()
