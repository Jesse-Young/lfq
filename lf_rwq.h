#ifndef _FREERWQ_H
#define _FREERWQ_H

#include <atomic_user.h>

typedef struct {
	volatile u64 r_idx;
    volatile u64 w_idx;
    int len;
    u32 q_pow;
    u32 blk_len;
    u32 blk_pow;
    u32 readers;
    u32 blk_cnt;
    int r_permit;
    int r_pmt_sgest;//suggest
    u64 dbg_r_total;
    u64 dbg_p_total;
    u64 dbg_get_pmt_total;
    volatile u64 *r_cnt;
    volatile u64 q[0];
} lfrwq_t;

typedef int (*lf_inq)(lfrwq_t* qh, void *data);

//int lfrwq_deq(lfrwq_t* qh, u32 r_permit, processfn *callback);

int lfrwq_get_token(lfrwq_t* qh);

int lfrwq_return_token(lfrwq_t* qh);

u64 lfrwq_deq(lfrwq_t* qh, void **ppdata);

int lfrwq_inq(lfrwq_t* qh, void *data);

int lfrwq_inq_m(lfrwq_t* qh, void *data);

int lfrwq_get_rpermit(lfrwq_t* qh);

void lfrwq_add_rcnt(lfrwq_t* qh, u32 total, u32 cnt_idx);

lfrwq_t* lfrwq_init(u32 q_len, u32 blk_len, u32 readers);


#define lfrwq_debug(f, a...)	{ \
					printf ("LFRWQ DEBUG (%s, %d): %s:", \
						__FILE__, __LINE__, __func__); \
				  	printf (f, ## a); \
					}



#endif

