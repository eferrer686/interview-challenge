CREATE TABLE IF NOT EXISTS pronosticopormunicipiosgz_raw (
    id serial PRIMARY KEY,
    ides integer,
    idmun integer,
    nes varchar,
    nmun varchar,
    dloc varchar,
    ndia integer,
    tmax numeric,
    tmin numeric,
    desciel varchar,
    probprec numeric,
    raf numeric,
    prec numeric,
    velvien numeric,
    dirvienc varchar,
    dirvieng numeric,
    cc numeric,
    lat numeric,
    lon numeric,
    dh varchar,
    log_timestamp timestamp DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pronosticopormunicipiosgz_latest (
    id serial PRIMARY KEY,
    ides integer,
    idmun integer,
    nes varchar,
    nmun varchar,
    dloc varchar,
    ndia integer,
    tmax numeric,
    tmin numeric,
    desciel varchar,
    probprec numeric,
    raf numeric,
    prec numeric,
    velvien numeric,
    dirvienc varchar,
    dirvieng numeric,
    cc numeric,
    lat numeric,
    lon numeric,
    dh varchar,
    log_timestamp timestamp DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS latest_temp_avg_by_mun(
    id serial PRIMARY KEY,
    idmun integer,
    tmax numeric,
    tmin numeric,
    log_timestamp timestamp DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS datos_municipios(
    idmun integer,
    ides integer,
    value numeric
);