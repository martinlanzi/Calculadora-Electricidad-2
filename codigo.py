from flask import Flask, render_template, request, redirect, url_for, session
import pandas as pd
from datetime import datetime, timedelta
import io
import contextlib
import math

app = Flask(__name__)
app.secret_key = "CEFIPelec2025calc"

def cargar_df_min(filepath):
    df = pd.read_excel(filepath)
    df.columns = df.columns.str.strip().str.lower()
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
    return df

# Cargar bases
zonas = cargar_df_min('Bases/Regiones/zonas_consesion.xlsx')

@app.route('/')
def index():
    form_data = session.pop('form_data', None)
    if form_data:
        return render_template(
            'index.html',
            output=form_data.get('output', ''),
            fecha_1=form_data.get('fecha_1', ''),
            fecha_2=form_data.get('fecha_2', ''),
            consumo=form_data.get('consumo', ''),
            provincia=form_data.get('provincia', ''),
            departamento=form_data.get('departamento', ''),
            nivel_ingresos=form_data.get('nivel_ingresos', ''),
            tsocial=form_data.get('tsocial', '')
        )
    else:
        return render_template('index.html')

@app.route('/calcular', methods=['POST'])
def calcular():
    try:
        fecha_1 = request.form['fecha_1']
        fecha_2 = request.form['fecha_2']
        consumo = request.form['consumo']
        provincia = request.form['provincia']
        departamento = request.form['departamento']
        nivel_ingresos = request.form['nivel_ingresos']
        tsocial = request.form['tsocial']

        # Capturar prints
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            resultado = calcular_tarifa(
                fecha_1, fecha_2, consumo, provincia, departamento,
                nivel_ingresos, tsocial, zonas
            )

        output = buffer.getvalue()

        # Guardar en sesión y redirigir
        session['form_data'] = {
            'output': output,
            'fecha_1': fecha_1,
            'fecha_2': fecha_2,
            'consumo': consumo,
            'provincia': provincia,
            'departamento': departamento,
            'nivel_ingresos': nivel_ingresos,
            'tsocial': tsocial
        }
        return redirect(url_for('index'))

    except Exception as e:
        session['form_data'] = {
            'output': f"Error:\n{str(e)}",
            'fecha_1': request.form.get('fecha_1', ''),
            'fecha_2': request.form.get('fecha_2', ''),
            'consumo': request.form.get('consumo', ''),
            'provincia': request.form.get('provincia', ''),
            'departamento': request.form.get('departamento', ''),
            'nivel_ingresos': request.form.get('nivel_ingresos', ''),
            'tsocial': request.form.get('tsocial', '')
        }
        return redirect(url_for('index'))

def calcular_tarifa(fecha_1, fecha_2, consumo, provincia, departamento, nivel_ingresos, tsocial, zonas):
   
    # Convertir y limpiar parámetros
    consumo = float(consumo)
    nivel_ingresos = int(nivel_ingresos)
    tsocial = int(tsocial)
    provincia = provincia.strip().lower()
    departamento = departamento.strip().lower()
    fecha_1 = datetime.strptime(fecha_1, '%d-%m-%Y')
    fecha_2 = datetime.strptime(fecha_2, '%d-%m-%Y')

    # Buscar la empresa distribuidora
    filtro_zonas = zonas.loc[
        (zonas['provincia'].str.strip().str.lower() == provincia) & 
        (zonas['municipio'].str.strip().str.lower() == departamento),
        'empresa'
    ]
    if filtro_zonas.empty:
        raise ValueError(f"No se encontró una distribuidora para la provincia '{provincia}' y el municipio '{departamento}'.")
    empresa = filtro_zonas.values[0].strip().lower()

    # Genera el id para cada empresa (idd = ID de la distribuidora, ids = ID de la subdistribuidora, si aplica)
    idd_map = {
        'edenor': "01",
        'edesur': "01",
        'edea': "03",
        'eden': "04",
        'edes': "05",
        'edelap': "06",
        'epec': "10",   # Córdoba
        'epesf': "25"     # Santa Fe
    }

    ids_map = {
        'edenor': "02",
        'edesur': "01",
        'edea': "01",
        'eden': "01",
        'edes': "01",
        'edelap': "01",
        'epec': "01",
        'epesf': "01"
    }

    idd = idd_map.get(empresa)
    ids = ids_map.get(empresa)

    ctarifarios = cargar_df_min(f'Bases/Tarifas/base_e_d{idd}_s{ids}_v01_res.xlsx')
    ctarifarios['costo_fijo'] = pd.to_numeric(ctarifarios['costo_fijo'], errors='coerce')
    ctarifarios['costo_variable'] = pd.to_numeric(ctarifarios['costo_variable'], errors='coerce')
    ctarifarios['umbral_minimo'] = pd.to_numeric(ctarifarios['umbral_minimo'], errors='coerce')
    ctarifarios['umbral_maximo'] = pd.to_numeric(ctarifarios['umbral_maximo'], errors='coerce')
    
    # Tratar NaN de manera diferenciada
    if 'umbral_minimo' in ctarifarios.columns:
        ctarifarios['umbral_minimo'] = ctarifarios['umbral_minimo'].fillna(0)

    if 'umbral_maximo' in ctarifarios.columns:
        ctarifarios['umbral_maximo'] = ctarifarios['umbral_maximo'].fillna(float('inf'))

    for col in ['costo_fijo', 'costo_variable', 'excedente', 'exc_cf']:
        if col in ctarifarios.columns:
            ctarifarios[col] = ctarifarios[col].fillna(0)


    fechas = cargar_df_min(f'Bases/Fechas/basef_e_d{idd}_s{ids}_v01_res.xlsx')

    fechas['desde'] = pd.to_datetime(fechas['desde'], errors='coerce', dayfirst=False)
    fechas['hasta'] = pd.to_datetime(fechas['hasta'], errors='coerce', dayfirst=False)
    fechas = fechas.dropna(subset=['desde'])
    fechas['hasta'] = fechas['hasta'].fillna(pd.to_datetime('2099-12-31'))
    
    # Elegir un 'ct' para cada fecha
    ct_col = 'archivo' if 'archivo' in fechas.columns else None
    if ct_col is None:
        raise ValueError("No se encuentra la columna 'archivo' en el dataframe de fechas")
    
    if empresa in ['edelap', 'edenor', 'edesur']:
       rango_fechas = pd.date_range(start=fecha_1, end=fecha_2, freq='D')
    else:
        rango_fechas = pd.date_range(start=fecha_1, end=fecha_2 - timedelta(days=1), freq='D')

    subperiodos = []
    for dia in rango_fechas:
        # CAMBIO: Eliminado el filtro redundante por empresa
        fila = fechas[(fechas['desde'] <= dia) & (fechas['hasta'] >= dia)]
        
        # CAMBIO: Simplificada la selección de CT - solo por fecha
        if not fila.empty:
            subperiodos.append({'fecha': dia, 'ct': fila.iloc[0][ct_col]})
        else:
            subperiodos.append({'fecha': dia, 'ct': None})

    subperiodos_df = pd.DataFrame(subperiodos)
    
    #print(subperiodos_df['ct'].dropna().unique())

    #subperiodos_df.to_excel('subperiodos_df.xlsx', index=False)

    if empresa in ['edesur','edenor']:
        dias_entre_lecturas = len(rango_fechas) 
        J = min(math.ceil(dias_entre_lecturas / 2), 31)
        consumo_a_facturar = int((consumo / dias_entre_lecturas) * J)
        cat_consumo = math.ceil((consumo / dias_entre_lecturas) * 30.5)

    elif empresa in ['edelap','edes', 'eden']:
        dias_entre_lecturas = len(rango_fechas)
        consumo_a_facturar = round(consumo)
        cat_consumo = consumo_a_facturar
    
    elif empresa == 'edea':
        dias_entre_lecturas = len(rango_fechas)
        consumo_a_facturar = math.floor(consumo / 2) 
        cat_consumo = int((consumo / dias_entre_lecturas) * 30.5)

    if empresa in ['edesur','edenor']:
       
        # Dividir en subperíodos
        fecha_subperiodo_1_fin = fecha_1 + timedelta(days=29)
        fecha_subperiodo_2_inicio = fecha_subperiodo_1_fin + timedelta(days=1)
        subperiodo_1 = subperiodos_df[(subperiodos_df['fecha'] >= fecha_1) & (subperiodos_df['fecha'] <= fecha_subperiodo_1_fin)]
        subperiodo_2 = subperiodos_df[(subperiodos_df['fecha'] >= fecha_subperiodo_2_inicio) & (subperiodos_df['fecha'] <= fecha_2)]

        # Cálculo de costos fijos
        cft_total_1 = 0
        for ct in subperiodo_1['ct'].dropna().unique():
            # CAMBIO: Agregado filtro por tarifa_social
            filtro = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['umbral_minimo'] <= cat_consumo) &
                (ctarifarios['umbral_maximo'] >= cat_consumo) &
                (ctarifarios['tarifa_social'] == tsocial)
            ]
            if not filtro.empty:
                dias_ct = subperiodo_1[subperiodo_1['ct'] == ct].shape[0]
                cft_total_1 += filtro['costo_fijo'].iloc[0] * (dias_ct / 31)

        dias_totales_subperiodo_2 = (fecha_2 - fecha_subperiodo_2_inicio).days + 1
        cft_total_2 = 0
        for ct in subperiodo_2['ct'].dropna().unique():
            filtro = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['umbral_minimo'] <= cat_consumo) &
                (ctarifarios['umbral_maximo'] >= cat_consumo) &
                (ctarifarios['tarifa_social'] == tsocial)
            ]
            if not filtro.empty:
                dias_ct = subperiodo_2[subperiodo_2['ct'] == ct].shape[0]
                cft_total_2 += filtro['costo_fijo'].iloc[0] * (dias_ct / dias_totales_subperiodo_2)

        cargo_fijo_1 = (cft_total_1 * J) / 30.5
        cargo_fijo_2 = (cft_total_2 * J) / 30.5

        # Cargos Variables

        cvt_total_1 = 0
        for ct in subperiodo_1['ct'].dropna().unique():
            dias_ct = subperiodo_1[subperiodo_1['ct'] == ct].shape[0]
            consumo_ct = consumo_a_facturar * (dias_ct / 31)
            filtro_ct = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['tarifa_social'] == tsocial)
            ].sort_values('umbral_minimo')
            if not filtro_ct.empty:
                for _, fila in filtro_ct.iterrows():
                    if consumo_ct <= fila['umbral_maximo'] or pd.isna(fila['umbral_maximo']):
                        tramo = round(fila['costo_variable'] * consumo_ct, 4)
                        cvt_total_1 += tramo
                        #print(f"[EDENOR/EDESUR] Subperíodo 1 - CT: {ct} | Días: {dias_ct} | Consumo: {consumo_ct:.2f} kWh | Precio: {fila['costo_variable']} $/kWh | Tramo: {tramo}")
                        break

        cvt_total_2 = 0
        for ct in subperiodo_2['ct'].dropna().unique():
            dias_ct = subperiodo_2[subperiodo_2['ct'] == ct].shape[0]
            consumo_ct = consumo_a_facturar * (dias_ct / dias_totales_subperiodo_2)
            filtro_ct = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['tarifa_social'] == tsocial)
            ].sort_values('umbral_minimo')
            if not filtro_ct.empty:
                for _, fila in filtro_ct.iterrows():
                    if consumo_ct <= fila['umbral_maximo'] or pd.isna(fila['umbral_maximo']):
                        tramo = round(fila['costo_variable'] * consumo_ct, 4)
                        cvt_total_2 += tramo
                        #print(f"[EDENOR/EDESUR] Subperíodo 2 - CT: {ct} | Días: {dias_ct} | Consumo: {consumo_ct:.2f} kWh | Precio: {fila['costo_variable']} $/kWh | Tramo: {tramo}")
                        break

        cvt_total_1 = (cvt_total_1 * J) / 30.5
        cvt_total_2 = (cvt_total_2 * J) / 30.5
        cvt_total_ponderado = cvt_total_1 + cvt_total_2

    elif empresa in ['edelap', 'edes', 'eden', 'edea']:
        
        # Cálculo de costos fijos
        cft_total = 0
        for ct in subperiodos_df['ct'].dropna().unique():
            # CAMBIO: Eliminado filtro redundante por empresa
            filtro = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['umbral_minimo'] <= cat_consumo) &
                (ctarifarios['umbral_maximo'] >= cat_consumo) &
                (ctarifarios['tarifa_social'] == tsocial)
            ]
            if not filtro.empty:
                dias_ct = subperiodos_df[subperiodos_df['ct'] == ct].shape[0]
                if empresa == 'edea':
                    ponderador_ct = dias_ct / dias_entre_lecturas
                    tramo = round(filtro['costo_fijo'].iloc[0] * ponderador_ct, 2)
                else:
                    tramo = round(filtro['costo_fijo'].iloc[0] * (dias_ct / dias_entre_lecturas), 2)
                cft_total += tramo
        if empresa == 'edea':
            cargo_fijo = round((cft_total * dias_entre_lecturas) / 61, 2)
        else:
            cargo_fijo = cft_total


        # Variables

        precios_pesados = []
        for ct in subperiodos_df['ct'].dropna().unique():
            dias_ct = subperiodos_df[subperiodos_df['ct'] == ct].shape[0]
            # CAMBIO: Agregados filtros por nivel_ingreso y tarifa_social
            filtro_ct = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['tarifa_social'] == tsocial)
            ].sort_values('umbral_minimo')
            if not filtro_ct.empty:
                for _, fila in filtro_ct.iterrows():
                    if consumo_a_facturar <= fila['umbral_maximo'] or pd.isna(fila['umbral_maximo']):
                        precios_pesados.append((fila['costo_variable'], dias_ct))
                        #print(f"[{empresa.upper()}] CT: {ct} | Precio: {fila['costo_variable']} $/kWh | Días: {dias_ct}")
                        break
        if precios_pesados:
            #print(f"dias_totales: {dias_entre_lecturas}")
            precio_promedio = sum(p * d for p, d in precios_pesados) / dias_entre_lecturas
            cargo_variable = consumo_a_facturar * precio_promedio
            #print(f"[{empresa.upper()}] Precio promedio ponderado: {precio_promedio:.4f} $/kWh | Consumo: {consumo_a_facturar} kWh")
        else:
            raise ValueError(f"No se encontraron precios variables válidos para los CTs en {empresa.upper()}")
        cvt_total = cargo_variable
    
    # EPEC 

    if empresa == 'epec':
        dias_entre_lecturas = len(rango_fechas)
        consumo_a_facturar = int(round(consumo / 2))
        cuota = 1 if (fecha_1.day <= 15) else 2
        cft_total = 0
        for ct in subperiodos_df['ct'].dropna().unique():
            # CAMBIO: Agregados filtros por nivel_ingreso y tarifa_social
            f = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['umbral_minimo'] <= consumo_a_facturar) &
                (ctarifarios['umbral_maximo'] >= consumo_a_facturar) &
                (ctarifarios['tarifa_social'] == tsocial)
            ]
            if f.empty: continue
            dias_ct = subperiodos_df[subperiodos_df['ct'] == ct].shape[0]
            tramo = f['costo_fijo'].iloc[0] * (dias_ct / dias_entre_lecturas)
            cft_total += tramo
        cargo_fijo = math.ceil(cft_total) if cuota == 1 else math.floor(cft_total)

        precios_pesados = []
        for ct in subperiodos_df['ct'].dropna().unique():
            dias_ct = subperiodos_df[subperiodos_df['ct'] == ct].shape[0]
            # CAMBIO: Agregados filtros por nivel_ingreso y tarifa_social
            f = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['tarifa_social'] == tsocial)
            ].sort_values('umbral_minimo')
            if f.empty: continue
            for _, fila in f.iterrows():
                if consumo_a_facturar <= fila['umbral_maximo'] or pd.isna(fila['umbral_maximo']):
                    precios_pesados.append((fila['costo_variable'], dias_ct))
                    break
        precio_promedio = sum(p*d for p,d in precios_pesados)/dias_entre_lecturas if precios_pesados else 0
        cargo_variable = consumo_a_facturar * precio_promedio
        cvt_total = cargo_variable

    # EPESF

    elif empresa == 'epesf':
        dias_entre_lecturas = len(rango_fechas)
        consumo_a_facturar = round(consumo)
        cft_total = 0
        for ct in subperiodos_df['ct'].dropna().unique():
            # CAMBIO: Agregados filtros por nivel_ingreso y tarifa_social
            f = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['umbral_minimo'] <= consumo_a_facturar) &
                (ctarifarios['umbral_maximo'] >= consumo_a_facturar) &
                (ctarifarios['tarifa_social'] == tsocial)
            ]
            if f.empty: continue
            dias_ct = subperiodos_df[subperiodos_df['ct'] == ct].shape[0]
            tramo = round(f['costo_fijo'].iloc[0] * (dias_ct / dias_entre_lecturas), 2)
            cft_total += tramo
        cargo_fijo = round(cft_total, 2)

        cvt_total = 0
        for ct in subperiodos_df['ct'].dropna().unique():
            dias_ct = subperiodos_df[subperiodos_df['ct'] == ct].shape[0]
            # CAMBIO: Agregados filtros por nivel_ingreso y tarifa_social
            f = ctarifarios[
                (ctarifarios['archivo'] == ct) &
                (ctarifarios['nivel_ingreso'] == nivel_ingresos) &
                (ctarifarios['tarifa_social'] == tsocial)
            ].sort_values('umbral_minimo')
            if f.empty: continue
            consumo_ct = consumo_a_facturar * (dias_ct / dias_entre_lecturas)
            acumulado = 0
            for _, fila in f.iterrows():
                umbral_max = fila['umbral_maximo']*2 if not pd.isna(fila['umbral_maximo']) else None
                if consumo_ct <= umbral_max or pd.isna(umbral_max):
                    tramo = fila['costo_variable'] * consumo_ct
                    acumulado += tramo
                    break
            cvt_total += acumulado
        cargo_variable = cvt_total

    # Resultados finales
    #print(f"\nEmpresa: {empresa}")
    #print(f"Consumo a facturar: {consumo_a_facturar:.2f} kWh")
    #print(f"Consumo categórico: {cat_consumo:.2f} kWh")
    if empresa in ['edesur','edenor']:
        print(f"Cargo fijo primera factura: {(cargo_fijo_1):.2f} $")
        print(f"Cargo fijo segunda factura: {(cargo_fijo_2):.2f} $")
        print(f"Cargo variable primera factura: {(cvt_total_1):.2f} $")
        print(f"Cargo variable segunda factura: {(cvt_total_2):.2f} $")
    else:
        print(f"Cargo fijo total: {cargo_fijo:.2f} $")
    if empresa in ['edesur','edenor']:
        print(f"Cargo variable total: {cvt_total_ponderado:.2f} $")
    else:
        print(f"Cargo variable total: {cargo_variable:.2f} $")
    if empresa in ['edesur','edenor']:
        print(f"TOTAL PRIMER FACTURA: {(cargo_fijo_1 + cvt_total_1):.2f} $")
        print(f"TOTAL SEGUNDA FACTURA: {(cargo_fijo_2 + cvt_total_2):.2f} $")
    else:
        print(f"Total factura: {(cargo_fijo + cvt_total):.2f} $")
        
if __name__ == '__main__':
    app.run(debug=True)
