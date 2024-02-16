#define __fhmix(h) ({                           \
      (h) ^= (h) >> 23;                         \
      (h) *= 0x2127599bf4325c37ULL;             \
      (h) ^= (h) >> 47; })

uint64_t fasthash64(const void *buf, size_t len, uint64_t seed)
{
	const uint64_t    m = 0x880355f21e6d1965ULL;
	const uint64_t *pos = (const uint64_t *)buf;
	const uint64_t *end = pos + (len / 8);
	const unsigned char *pos2;
	uint64_t h = seed ^ (len * m);
	uint64_t v;

	while (pos != end) {
		v  = *pos++;
		h ^= __fhmix(v);
		h *= m;
	}

	pos2 = (const unsigned char*)pos;
	v = 0;

	switch (len & 7) {
    case 7: v ^= (uint64_t)pos2[6] << 48;
      // fall through
    case 6: v ^= (uint64_t)pos2[5] << 40;
      // fall through
    case 5: v ^= (uint64_t)pos2[4] << 32;
      // fall through
    case 4: v ^= (uint64_t)pos2[3] << 24;
      // fall through
    case 3: v ^= (uint64_t)pos2[2] << 16;
      // fall through
    case 2: v ^= (uint64_t)pos2[1] << 8;
      // fall through
    case 1: v ^= (uint64_t)pos2[0];
      h ^= __fhmix(v);
      h *= m;
	}
	return __fhmix(h);
}
