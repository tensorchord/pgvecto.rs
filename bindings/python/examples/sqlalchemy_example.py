import os

import numpy as np
from sqlalchemy import Integer, String, create_engine, insert, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from pgvecto_rs.sqlalchemy import Vector

URL = "postgresql+psycopg://{username}:{password}@{host}:{port}/{db_name}".format(
    port=os.getenv("DB_PORT", "5432"),
    host=os.getenv("DB_HOST", "localhost"),
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "mysecretpassword"),
    db_name=os.getenv("DB_NAME", "postgres"),
)


# Define the ORM model
class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(String)
    embedding: Mapped[np.ndarray] = mapped_column(Vector(3))

    def __repr__(self) -> str:
        return f"{self.text}: {self.embedding}"


# Connect to the DB and create the table
engine = create_engine(URL)
Document.metadata.create_all(engine)

with Session(engine) as session:
    # Insert 3 rows into the table
    t1 = insert(Document).values(text="hello world", embedding=[1, 2, 3])
    t2 = insert(Document).values(text="hello postgres", embedding=[1.0, 2.0, 4.0])
    t3 = insert(Document).values(text="hello pgvecto.rs", embedding=np.array([1, 3, 4]))
    for t in [t1, t2, t3]:
        session.execute(t)
    session.commit()

    # Select the row "hello pgvecto.rs"
    stmt = select(Document).where(Document.text == "hello pgvecto.rs")
    target = session.scalar(stmt)

    # Select all the rows and sort them
    # by the squared_euclidean_distance to "hello pgvecto.rs"
    stmt = select(
        Document.text,
        Document.embedding.squared_euclidean_distance(target.embedding).label(
            "distance",
        ),
    ).order_by("distance")
    for doc in session.execute(stmt):
        print(doc)

    # The output will be:
    # ```
    # ('hello pgvecto.rs', 0.0)
    # ('hello postgres', 1.0)
    # ('hello world', 2.0)
    # ```

# Drop the table
Document.metadata.drop_all(engine)
