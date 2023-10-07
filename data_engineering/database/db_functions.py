from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from pandas import DataFrame
import pandas as pd


class Base(DeclarativeBase):
    pass

class SecurityMaster(Base):
    __tablename__ = "SecurityMaster"
    __table_args__ = {"schema": "dbo"}
    SecID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    Ticker: Mapped[str] = mapped_column()
    Sector: Mapped[str] = mapped_column()
    Country: Mapped[str] = mapped_column()
    SecurityType: Mapped[str] = mapped_column()
    SecurityName: Mapped[str] = mapped_column()
    
class MarketData(Base):
    __tablename__ = "MarketData"
    __table_args__ = {"schema": "dbo"}
    MD_ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    AsOfDate: Mapped[str] = mapped_column()
    SecID: Mapped[int] = mapped_column()
    Open: Mapped[float] = mapped_column()
    High: Mapped[float] = mapped_column()
    Low: Mapped[float] = mapped_column()
    Close: Mapped[float] = mapped_column()
    Volume: Mapped[int] = mapped_column()
    Dividends: Mapped[float] = mapped_column()
    Stock_Splits: Mapped[float] = mapped_column()
    Dataload_Date: Mapped[str] = mapped_column()
    
class Portfolio(Base):
    __tablename__ = "Portfolio"
    __table_args__ = {"schema": "dbo"}
    PID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    PortfolioShortName: Mapped[str] = mapped_column()
    PortfolioName : Mapped[str] = mapped_column()

class PortfolioHoldings(Base):
    __tablename__ = "PortfolioHoldings"
    __table_args__ = {"schema": "dbo"}
    HID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    AsOfDate: Mapped[str] = mapped_column()
    PortID: Mapped[int] = mapped_column()
    SecID: Mapped[int] = mapped_column()
    HeldShares: Mapped[float] = mapped_column()
    Price: Mapped[float] = mapped_column()
    MarketValue: Mapped[float] = mapped_column()
    Weight: Mapped[float] = mapped_column()
    


def read_security_master(orm_session: Session, orm_engine: Engine) -> DataFrame:
    df_securityMaster = pd.read_sql_query(
        sql=orm_session.query(
            SecurityMaster.SecID,
            SecurityMaster.Ticker,
            SecurityMaster.Sector,
            SecurityMaster.Country,
            SecurityMaster.SecurityType,
            SecurityMaster.SecurityName,
        ).statement,
        con=orm_engine,
    )
    return df_securityMaster


def write_dataframe_to_marketdata(market_data: DataFrame,orm_session: Session, orm_engine: Engine):
    try:

        data_list = market_data.to_dict(orient='records')
        as_of_dates = market_data['AsOfDate'].unique().tolist()
        
        orm_session.query(MarketData).filter(MarketData.AsOfDate.in_(as_of_dates)).delete(synchronize_session=False)
        orm_session.bulk_insert_mappings(MarketData, data_list)
        orm_session.commit()

    except SQLAlchemyError as e:
        print(f"An error occurred: {str(e)}")
        orm_session.rollback() 
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        orm_session.rollback() 
    finally:
        orm_session.close()

