import polars as pl



from numba import njit
import numpy as np

@njit
def ols_beta_resid_r2(y, X):
    n = X.shape[0]
    ones = np.ones((n, 1))
    X = np.concatenate((ones, X), axis=1)

    XtX = X.T @ X
    try:
        XtX_inv = np.linalg.inv(XtX)
    except:
        return (
            np.full(X.shape[1] - 1, np.nan),  # beta
            np.nan,                           # intercept
            np.full_like(y, np.nan),          # resid
            np.nan,                           # r2
            np.full(X.shape[1] - 1, np.nan)   # se
        )

    beta = XtX_inv @ X.T @ y
    y_hat = X @ beta
    resid = y - y_hat

    ss_res = np.sum(resid ** 2)
    k = X.shape[1]  # number of parameters including intercept
    sigma2 = ss_res / (n - k) if n > k else np.nan

    var_beta = sigma2 * XtX_inv  # full variance-covariance matrix
    se = np.sqrt(np.diag(var_beta))[1:] if sigma2 == sigma2 else np.full(X.shape[1] - 1, np.nan)  # exclude intercept

    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

    return beta[1:], beta[0], resid, r2, se

@njit
def wls_beta_resid_r2(y, X, w):
    """
    加权最小二乘（WLS）回归
    返回：beta（不含截距）, intercept, resid, r², 标准误差se
    """
    n, p = X.shape
    ones = np.ones((n, 1))
    X = np.concatenate((ones, X), axis=1)  # 添加截距项
    p += 1  # 加上 intercept 的维度

    W = np.diag(w)

    XtWX = X.T @ W @ X
    XtWy = X.T @ W @ y

    try:
        XtWX_inv = np.linalg.inv(XtWX)
    except:
        return (np.full(p - 1, np.nan),  # beta (不含截距)
                np.nan,                 # intercept
                np.full_like(y, np.nan),
                np.nan,
                np.full(p - 1, np.nan))  # se

    beta = XtWX_inv @ XtWy
    y_hat = X @ beta
    resid = y - y_hat

    # 加权 R²
    y_weighted_mean = np.average(y, weights=w)
    ss_res = np.sum(w * resid**2)
    ss_tot = np.sum(w * (y - y_weighted_mean) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

    # 加权方差估计 & 标准误差
    sigma2 = ss_res / (n - p)
    se_all = np.sqrt(np.diag(XtWX_inv) * sigma2)  # 包括截距的se

    return beta[1:], beta[0], resid, r2, se_all[1:]  # 不返回截距的SE


def ols_polars_fn(x_cols: list[str], y_col: str, return_type: str = "beta"):
    def _fn(df: pl.DataFrame) -> pl.DataFrame:
        if len(df) < len(x_cols) + 1:
            if return_type == "resid":
                return df.with_columns(pl.lit(np.nan).alias("resid"))
            else:
                return pl.DataFrame({return_type: [np.nan]})

        X = np.stack([df[col].to_numpy() for col in x_cols], axis=1)
        y = df[y_col].to_numpy()

        beta, intercept, resid, r2 ,se= ols_beta_resid_r2(y, X)

        if return_type == "beta":
            return pl.DataFrame({
                "date": [df["date"][0]],
                **{f"beta_{col}": [b] for col, b in zip(x_cols, beta)},
                **{f"se_{col}": [s] for col, s in zip(x_cols, se)}
            })
        elif return_type == "r2":
            return pl.DataFrame({ "date": [df["date"][0]],
                                  "r2": [r2]})
        elif return_type == "resid":
            return df.with_columns(pl.Series("resid", resid))
        else:
            raise ValueError(f"Unsupported return_type: {return_type}")
    return _fn

def wls_polars_fn(x_cols: list[str], y_col: str, w_col:str,return_type: str = "beta"):
    def _fn(df: pl.DataFrame) -> pl.DataFrame:
        if len(df) < len(x_cols) + 1:
            if return_type == "resid":
                return df.with_columns(pl.lit(np.nan).alias("resid"))
            else:
                return pl.DataFrame({return_type: [np.nan]})

        X = np.stack([df[col].to_numpy() for col in x_cols], axis=1)
        y = df[y_col].to_numpy()
        w = df[w_col].to_numpy()

        beta, intercept, resid, r2,se = wls_beta_resid_r2(y, X,w)

        if return_type == "beta":
            return pl.DataFrame({
                "date": [df["date"][0]],
                **{f"beta_{col}": [b] for col, b in zip(x_cols, beta)},
                **{f"se_{col}": [s] for col, s in zip(x_cols, se)}
            })
        elif return_type == "r2":
            return pl.DataFrame({
                 "date": [df["date"][0]],
                "r2": [r2]})
        elif return_type == "resid":
            return df.with_columns(pl.Series("resid", resid))
        else:
            raise ValueError(f"Unsupported return_type: {return_type}")
    return _fn

