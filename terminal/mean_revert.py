import argparse
import config as cfg
import db
import matplotlib.pyplot as plt
from datetime import datetime
import quant
import numpy as np

config = cfg.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pair', type=str)
    parser.add_argument('--w', type=int, default=40)
    parser.add_argument('--dt', type=int, default=86400)
    parser.add_argument('--n', type=int, default=100)
    parser.add_argument('--bottom', type=float, default=None)
    args = parser.parse_args()
    with db.get_connection() as conn:
        rows = db.all(conn, f"""
            select 
                (time/{args.dt})::integer*{args.dt} as dt,
                (array_agg(price ORDER BY time ASC))[1] o,
                MAX(price) h,
                MIN(price) l,
                (array_agg(price ORDER BY time DESC))[1] c	
            from kraken_{args.pair}_trades 
            where time >= (extract(epoch from now())/{args.dt})::integer*{args.dt} - {args.dt}*{args.n}
            group by (time/{args.dt})::integer
            limit {args.n}
        """)
        dt, cl = zip(*[(datetime.utcfromtimestamp(r['dt']), r['c']) for r in rows])
        cl = np.array(cl).astype('float64')
        cl_sma = quant.sma(cl, args.w)

        fig = plt.figure()

        plt.subplot(2,1,1)
        plt.plot(dt, cl)
        plt.plot(dt, cl_sma)
        plt.xticks(rotation=45)

        plt.subplot(2,1,2)
        plt.plot(dt, cl/cl_sma)
        if args.bottom:
            plt.axhline(y=args.bottom, color='g')
        plt.xticks(rotation=45)

        fig.tight_layout()

        plt.show()
