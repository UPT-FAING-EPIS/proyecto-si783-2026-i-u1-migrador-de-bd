import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import threading
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from extraccion.conector_multibase import ConectorMultiBase
from extraccion.conector_base_datos import ConectorBaseDatos
from extraccion.descubridor_esquema import DescubridorEsquema
from extraccion.extractor_lotes import ExtractorLotes
from transformacion.mapeador_campos import MapeadorCampos
from carga.cargador_masivo import CargadorMasivo
from carga.validador_post_migracion import ValidadorPostMigracion

class AplicacionMigrador:
    def __init__(self, ventana):
        self.ventana = ventana
        self.ventana.title("Sistema Migrador de Bases de Datos")
        self.ventana.geometry("1100x750")
        self.ventana.configure(bg='#f0f0f0')
        
        # Variables
        self.bd_origen = tk.StringVar(value="PostgreSQL")
        self.bd_destino = tk.StringVar(value="PostgreSQL")
        self.host_origen = tk.StringVar(value="localhost")
        self.host_destino = tk.StringVar(value="localhost")
        self.puerto_origen = tk.StringVar(value="5432")
        self.puerto_destino = tk.StringVar(value="5432")
        self.usuario_origen = tk.StringVar(value="postgres")
        self.usuario_destino = tk.StringVar(value="postgres")
        self.contrasena_origen = tk.StringVar()
        self.contrasena_destino = tk.StringVar()
        self.nombre_bd_origen = tk.StringVar(value="bd_origen")
        self.nombre_bd_destino = tk.StringVar(value="bd_destino")
        
        self.proceso_activo = False
        self.proceso_pausado = False
        self.conector_origen = None
        self.conector_destino = None
        self.esquema_descubierto = None
        self.tablas_a_migrar = []
        self.tabla_actual = ""
        
        # Actualizar puertos cuando cambia la BD
        self.bd_origen.trace('w', lambda *args: self.actualizar_puerto('origen'))
        self.bd_destino.trace('w', lambda *args: self.actualizar_puerto('destino'))
        
        self.crear_interfaz()
        
    def actualizar_puerto(self, tipo):
        bd = self.bd_origen.get() if tipo == 'origen' else self.bd_destino.get()
        info = ConectorMultiBase.BASES_SOPORTADAS.get(bd, {})
        puerto = str(info.get('puerto_default', ''))
        if tipo == 'origen':
            self.puerto_origen.set(puerto)
        else:
            self.puerto_destino.set(puerto)
        
    def crear_interfaz(self):
        # Estilo
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('TFrame', background='#f0f0f0')
        style.configure('TLabel', background='#f0f0f0', foreground='#333333', font=('Segoe UI', 9))
        style.configure('TLabelframe', background='#f0f0f0', foreground='#333333')
        style.configure('TLabelframe.Label', background='#f0f0f0', foreground='#333333', font=('Segoe UI', 10, 'bold'))
        style.configure('TButton', background='#e0e0e0', foreground='#333333', font=('Segoe UI', 9))
        style.configure('TEntry', fieldbackground='white', foreground='#333333')
        style.configure('TCombobox', fieldbackground='white', foreground='#333333')
        
        # Notebook para pestañas
        notebook = ttk.Notebook(self.ventana)
        notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Pestaña 1: Configuración
        pestana_config = ttk.Frame(notebook)
        notebook.add(pestana_config, text='Configuracion')
        self.crear_pestana_configuracion(pestana_config)
        
        # Pestaña 2: Migración
        pestana_migracion = ttk.Frame(notebook)
        notebook.add(pestana_migracion, text='Migracion')
        self.crear_pestana_migracion(pestana_migracion)
        
        # Pestaña 3: Logs
        pestana_logs = ttk.Frame(notebook)
        notebook.add(pestana_logs, text='Registro de Actividad')
        self.crear_pestana_logs(pestana_logs)
        
        # Barra de estado
        self.barra_estado = tk.Label(self.ventana, text="  Sistema listo", 
                                     bg='#2196F3', fg='white', anchor='w', pady=5,
                                     font=('Segoe UI', 9))
        self.barra_estado.pack(fill='x', side='bottom')
    
    def crear_pestana_configuracion(self, padre):
        # Frame izquierdo - Origen
        frame_origen = ttk.LabelFrame(padre, text="BASE DE DATOS ORIGEN", padding=15)
        frame_origen.pack(side='left', fill='both', expand=True, padx=10, pady=10)
        
        bases = list(ConectorMultiBase.BASES_SOPORTADAS.keys())
        
        ttk.Label(frame_origen, text="Motor:").pack(anchor='w', pady=(5,0))
        combo_origen = ttk.Combobox(frame_origen, textvariable=self.bd_origen, values=bases, width=25)
        combo_origen.pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_origen, text="Host:").pack(anchor='w')
        ttk.Entry(frame_origen, textvariable=self.host_origen).pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_origen, text="Puerto:").pack(anchor='w')
        ttk.Entry(frame_origen, textvariable=self.puerto_origen).pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_origen, text="Usuario:").pack(anchor='w')
        ttk.Entry(frame_origen, textvariable=self.usuario_origen).pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_origen, text="Contrasena:").pack(anchor='w')
        ttk.Entry(frame_origen, textvariable=self.contrasena_origen, show="*").pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_origen, text="Nombre de Base de Datos:").pack(anchor='w')
        ttk.Entry(frame_origen, textvariable=self.nombre_bd_origen).pack(fill='x', pady=(0,10))
        
        btn_probar_origen = tk.Button(frame_origen, text="Probar Conexion Origen", 
                                      bg='#4CAF50', fg='white', font=('Segoe UI', 9, 'bold'),
                                      command=lambda: self.probar_conexion('origen'),
                                      padx=15, pady=5, cursor='hand2')
        btn_probar_origen.pack(pady=5)
        
        # Frame derecho - Destino
        frame_destino = ttk.LabelFrame(padre, text="BASE DE DATOS DESTINO", padding=15)
        frame_destino.pack(side='right', fill='both', expand=True, padx=10, pady=10)
        
        ttk.Label(frame_destino, text="Motor:").pack(anchor='w', pady=(5,0))
        combo_destino = ttk.Combobox(frame_destino, textvariable=self.bd_destino, values=bases, width=25)
        combo_destino.pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_destino, text="Host:").pack(anchor='w')
        ttk.Entry(frame_destino, textvariable=self.host_destino).pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_destino, text="Puerto:").pack(anchor='w')
        ttk.Entry(frame_destino, textvariable=self.puerto_destino).pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_destino, text="Usuario:").pack(anchor='w')
        ttk.Entry(frame_destino, textvariable=self.usuario_destino).pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_destino, text="Contrasena:").pack(anchor='w')
        ttk.Entry(frame_destino, textvariable=self.contrasena_destino, show="*").pack(fill='x', pady=(0,5))
        
        ttk.Label(frame_destino, text="Nombre de Base de Datos:").pack(anchor='w')
        ttk.Entry(frame_destino, textvariable=self.nombre_bd_destino).pack(fill='x', pady=(0,10))
        
        btn_probar_destino = tk.Button(frame_destino, text="Probar Conexion Destino",
                                       bg='#4CAF50', fg='white', font=('Segoe UI', 9, 'bold'),
                                       command=lambda: self.probar_conexion('destino'),
                                       padx=15, pady=5, cursor='hand2')
        btn_probar_destino.pack(pady=5)
        
        # Botones inferiores
        frame_botones = ttk.Frame(padre)
        frame_botones.pack(side='bottom', fill='x', pady=10)
        
        btn_descubrir = tk.Button(frame_botones, text="Descubrir Esquema de Origen",
                                  bg='#2196F3', fg='white', font=('Segoe UI', 9, 'bold'),
                                  command=self.descubrir_esquema,
                                  padx=15, pady=5, cursor='hand2')
        btn_descubrir.pack(side='left', padx=5)
        
        btn_crear = tk.Button(frame_botones, text="Crear Estructura en Destino",
                             bg='#FF9800', fg='white', font=('Segoe UI', 9, 'bold'),
                             command=self.crear_estructura_destino,
                             padx=15, pady=5, cursor='hand2')
        btn_crear.pack(side='left', padx=5)
    
    def crear_pestana_migracion(self, padre):
        # Informacion de configuracion
        frame_info = ttk.LabelFrame(padre, text="Configuracion Actual", padding=15)
        frame_info.pack(fill='x', padx=10, pady=10)
        
        self.label_info = tk.Label(frame_info, 
                                   text="Configure las bases de datos en la pestana Configuracion",
                                   font=('Segoe UI', 10), fg='#333333', bg='#f0f0f0')
        self.label_info.pack()
        
        # Progreso general
        frame_progreso_general = ttk.LabelFrame(padre, text="Progreso General", padding=15)
        frame_progreso_general.pack(fill='x', padx=10, pady=10)
        
        self.barra_progreso = ttk.Progressbar(frame_progreso_general, length=800, mode='determinate')
        self.barra_progreso.pack(pady=5)
        
        self.label_porcentaje = tk.Label(frame_progreso_general, text="0%", 
                                         font=('Segoe UI', 20, 'bold'), fg='#2196F3', bg='#f0f0f0')
        self.label_porcentaje.pack()
        
        self.label_estado = tk.Label(frame_progreso_general, text="Listo para migrar",
                                     font=('Segoe UI', 10), fg='#666666', bg='#f0f0f0')
        self.label_estado.pack()
        
        # Tabla actual
        self.label_tabla_actual = tk.Label(frame_progreso_general, text="",
                                           font=('Segoe UI', 11, 'bold'), fg='#333333', bg='#f0f0f0')
        self.label_tabla_actual.pack(pady=5)
        
        # Progreso de tabla actual
        self.barra_tabla = ttk.Progressbar(frame_progreso_general, length=600, mode='determinate')
        self.barra_tabla.pack(pady=5)
        
        # Estadisticas
        frame_stats = ttk.LabelFrame(padre, text="Estadisticas de Migracion", padding=15)
        frame_stats.pack(fill='x', padx=10, pady=10)
        
        # Primera fila de estadisticas
        frame_stats_1 = ttk.Frame(frame_stats)
        frame_stats_1.pack(fill='x', pady=5)
        
        ttk.Label(frame_stats_1, text="Tablas descubiertas:", font=('Segoe UI', 10, 'bold')).pack(side='left', padx=5)
        self.stat_tablas = tk.Label(frame_stats_1, text="0", font=('Segoe UI', 10), fg='#333333', bg='#f0f0f0')
        self.stat_tablas.pack(side='left', padx=5)
        
        ttk.Label(frame_stats_1, text="|", font=('Segoe UI', 10)).pack(side='left', padx=5)
        
        ttk.Label(frame_stats_1, text="Tablas migradas:", font=('Segoe UI', 10, 'bold')).pack(side='left', padx=5)
        self.stat_tablas_migradas = tk.Label(frame_stats_1, text="0", font=('Segoe UI', 10), fg='#4CAF50', bg='#f0f0f0')
        self.stat_tablas_migradas.pack(side='left', padx=5)
        
        ttk.Label(frame_stats_1, text="|", font=('Segoe UI', 10)).pack(side='left', padx=5)
        
        ttk.Label(frame_stats_1, text="Errores:", font=('Segoe UI', 10, 'bold')).pack(side='left', padx=5)
        self.stat_errores = tk.Label(frame_stats_1, text="0", font=('Segoe UI', 10), fg='#F44336', bg='#f0f0f0')
        self.stat_errores.pack(side='left', padx=5)
        
        # Segunda fila de estadisticas
        frame_stats_2 = ttk.Frame(frame_stats)
        frame_stats_2.pack(fill='x', pady=5)
        
        ttk.Label(frame_stats_2, text="Total registros extraidos:", font=('Segoe UI', 10, 'bold')).pack(side='left', padx=5)
        self.stat_extraidos = tk.Label(frame_stats_2, text="0", font=('Segoe UI', 10), fg='#333333', bg='#f0f0f0')
        self.stat_extraidos.pack(side='left', padx=5)
        
        ttk.Label(frame_stats_2, text="|", font=('Segoe UI', 10)).pack(side='left', padx=5)
        
        ttk.Label(frame_stats_2, text="Total registros cargados:", font=('Segoe UI', 10, 'bold')).pack(side='left', padx=5)
        self.stat_cargados = tk.Label(frame_stats_2, text="0", font=('Segoe UI', 10), fg='#333333', bg='#f0f0f0')
        self.stat_cargados.pack(side='left', padx=5)
        
        # Botones de control
        frame_control = ttk.Frame(padre)
        frame_control.pack(pady=10)
        
        self.btn_iniciar = tk.Button(frame_control, text="INICIAR MIGRACION COMPLETA",
                                     font=('Segoe UI', 12, 'bold'), bg='#4CAF50', fg='white',
                                     command=self.iniciar_migracion, padx=30, pady=10,
                                     cursor='hand2', width=25)
        self.btn_iniciar.pack(side='left', padx=10)
        
        self.btn_pausar = tk.Button(frame_control, text="PAUSAR",
                                    font=('Segoe UI', 10), bg='#FFC107', fg='#333333',
                                    command=self.pausar_migracion, padx=20, pady=8,
                                    cursor='hand2', state='disabled')
        self.btn_pausar.pack(side='left', padx=10)
        
        self.btn_reporte = tk.Button(frame_control, text="VER REPORTE FINAL",
                                     font=('Segoe UI', 10), bg='#2196F3', fg='white',
                                     command=self.ver_reporte, padx=20, pady=8,
                                     cursor='hand2')
        self.btn_reporte.pack(side='left', padx=10)
    
    def crear_pestana_logs(self, padre):
        self.area_logs = scrolledtext.ScrolledText(padre, bg='white', fg='#333333',
                                                   font=('Consolas', 9), wrap='word')
        self.area_logs.pack(fill='both', expand=True, padx=10, pady=10)
        
        frame_botones = ttk.Frame(padre)
        frame_botones.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(frame_botones, text="Limpiar", 
                  command=lambda: self.area_logs.delete('1.0', 'end')).pack(side='left', padx=5)
        ttk.Button(frame_botones, text="Exportar", 
                  command=self.exportar_logs).pack(side='left', padx=5)
    
    def log(self, mensaje):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.area_logs.insert('end', f"[{timestamp}] {mensaje}\n")
        self.area_logs.see('end')
        self.ventana.update_idletasks()
    
    def obtener_config_conexion(self, tipo):
        if tipo == 'origen':
            return {
                "motor": self.bd_origen.get(),
                "host": self.host_origen.get(),
                "puerto": self.puerto_origen.get(),
                "usuario": self.usuario_origen.get(),
                "contrasena": self.contrasena_origen.get(),
                "nombre_bd": self.nombre_bd_origen.get()
            }
        else:
            return {
                "motor": self.bd_destino.get(),
                "host": self.host_destino.get(),
                "puerto": self.puerto_destino.get(),
                "usuario": self.usuario_destino.get(),
                "contrasena": self.contrasena_destino.get(),
                "nombre_bd": self.nombre_bd_destino.get()
            }
    
    def probar_conexion(self, tipo):
        config = self.obtener_config_conexion(tipo)
        self.log(f"Probando conexion a {tipo.upper()} ({config['motor']})...")
        
        try:
            conector = ConectorBaseDatos(config)
            with conector.obtener_conexion() as conn:
                self.log(f"Conexion exitosa a {tipo.upper()}")
                
                if tipo == 'origen':
                    self.conector_origen = conector
                else:
                    self.conector_destino = conector
                
                messagebox.showinfo("Conexion Exitosa", f"Conectado a la base de datos {tipo}")
                self.barra_estado.config(text=f"  Conectado a {tipo.upper()}: {config['motor']}")
        except Exception as e:
            self.log(f"Error conectando a {tipo.upper()}: {str(e)}")
            messagebox.showerror("Error de Conexion", f"No se pudo conectar:\n{str(e)}")
    
    def descubrir_esquema(self):
        if not self.conector_origen:
            messagebox.showwarning("Advertencia", "Primero pruebe la conexion a la base de datos origen")
            return
        
        self.log("Descubriendo esquema completo de la base de datos origen...")
        
        try:
            descubridor = DescubridorEsquema(self.conector_origen)
            self.esquema_descubierto = descubridor.descubrir_esquema_completo()
            self.tablas_a_migrar = list(self.esquema_descubierto.get("tablas", {}).keys())
            
            self.log(f"ESQUEMA DESCUBIERTO: {len(self.tablas_a_migrar)} tablas encontradas")
            self.log("-" * 50)
            
            for tabla, info in self.esquema_descubierto.get("tablas", {}).items():
                self.log(f"  Tabla: {tabla} ({len(info['columnas'])} columnas, {len(info['claves_foraneas'])} relaciones)")
                
                # Mostrar columnas
                for col in info['columnas'][:5]:
                    pk = " [PK]" if col['nombre'] in info.get('claves_primarias', []) else ""
                    self.log(f"    - {col['nombre']} ({col['tipo']}){pk}")
                
                if len(info['columnas']) > 5:
                    self.log(f"    ... y {len(info['columnas']) - 5} columnas mas")
            
            descubridor.guardar_esquema(self.esquema_descubierto)
            self.log(f"\nEsquema guardado en 'esquema_descubierto.json'")
            
            # Actualizar UI
            self.stat_tablas.config(text=str(len(self.tablas_a_migrar)))
            self.label_info.config(text=f"Base de datos origen: {self.nombre_bd_origen.get()} ({len(self.tablas_a_migrar)} tablas)")
            
            messagebox.showinfo("Esquema Descubierto", 
                              f"Se encontraron {len(self.tablas_a_migrar)} tablas en la base de datos origen")
        except Exception as e:
            self.log(f"Error descubriendo esquema: {str(e)}")
            messagebox.showerror("Error", str(e))
    
    def crear_estructura_destino(self):
        if not self.esquema_descubierto:
            messagebox.showwarning("Advertencia", "Primero descubra el esquema de la base origen")
            return
        
        if not self.conector_destino:
            messagebox.showwarning("Advertencia", "Primero pruebe la conexion a la base de datos destino")
            return
        
        self.log("Creando estructura completa en base de datos destino...")
        
        try:
            descubridor = DescubridorEsquema(self.conector_destino)
            script = descubridor.generar_script_creacion(self.esquema_descubierto)
            
            with self.conector_destino.obtener_conexion() as conn:
                from sqlalchemy import text
                sentencias = [s.strip() for s in script.split(';') if s.strip()]
                
                for i, sentencia in enumerate(sentencias):
                    try:
                        conn.execute(text(sentencia))
                        self.log(f"  Ejecutada sentencia {i+1}/{len(sentencias)}")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            self.log(f"  Tabla ya existe (sentencia {i+1})")
                        else:
                            self.log(f"  Aviso en sentencia {i+1}: {str(e)[:100]}")
                
                conn.commit()
            
            self.log("Estructura creada exitosamente en destino")
            messagebox.showinfo("Estructura Creada", "Todas las tablas fueron creadas en la base de datos destino")
        except Exception as e:
            self.log(f"Error creando estructura: {str(e)}")
            messagebox.showerror("Error", str(e))
    
    def iniciar_migracion(self):
        if not self.conector_origen or not self.conector_destino:
            messagebox.showwarning("Advertencia", "Primero pruebe las conexiones a ambas bases de datos")
            return
        
        if not self.esquema_descubierto:
            messagebox.showwarning("Advertencia", "Primero descubra el esquema de la base de datos origen")
            return
        
        self.proceso_activo = True
        self.barra_progreso['value'] = 0
        self.label_porcentaje.config(text="0%")
        self.btn_iniciar.config(state='disabled', bg='#999999')
        self.btn_pausar.config(state='normal')
        
        self.log("=" * 60)
        self.log(f"INICIANDO MIGRACION COMPLETA: {self.bd_origen.get()} -> {self.bd_destino.get()}")
        self.log(f"Tablas a migrar: {len(self.tablas_a_migrar)}")
        self.log("=" * 60)
        
        self.metricas = {
            "extraidos": 0,
            "cargados": 0,
            "errores": 0,
            "tablas_migradas": 0,
            "tablas_con_error": []
        }
        
        hilo = threading.Thread(target=self.proceso_migracion, daemon=True)
        hilo.start()
    
    def proceso_migracion(self):
        total_tablas = len(self.tablas_a_migrar)
        
        for idx, tabla in enumerate(self.tablas_a_migrar):
            if not self.proceso_activo:
                break
            
            while self.proceso_pausado:
                self.ventana.after(100)
            
            self.tabla_actual = tabla
            progreso_general = int((idx / total_tablas) * 100)
            
            self.ventana.after(0, lambda t=tabla, p=progreso_general: self.actualizar_ui_tabla(t, p, 0, "extrayendo"))
            self.log(f"\n[{idx+1}/{total_tablas}] Migrando tabla: {tabla}")
            
            try:
                # Extraer datos de esta tabla
                config_extraccion = {
                    "tamano_lote": 1000,
                    "consulta_sql": f"SELECT * FROM {tabla}"
                }
                extractor = ExtractorLotes(self.conector_origen, config_extraccion)
                
                datos_extraidos = []
                for lote in extractor.extraer_por_lotes():
                    if not self.proceso_activo:
                        break
                    while self.proceso_pausado:
                        self.ventana.after(100)
                    datos_extraidos.extend(lote)
                    self.ventana.after(0, lambda e=len(datos_extraidos): self.actualizar_ui_tabla(
                        tabla, progreso_general, 50, f"extrayendo ({e} registros)"))
                
                if not self.proceso_activo:
                    break
                
                self.metricas["extraidos"] += len(datos_extraidos)
                self.ventana.after(0, lambda t=tabla, p=progreso_general: self.actualizar_ui_tabla(
                    t, p, 70, f"transformando ({len(datos_extraidos)} registros)"))
                
                # Mapear campos (el sistema lo hace automaticamente)
                mapeador = MapeadorCampos()
                columnas = self.esquema_descubierto["tablas"][tabla]["columnas"]
                for col in columnas:
                    mapeador.agregar_mapeo(col["nombre"], col["nombre"])
                
                datos_transformados = [mapeador.aplicar_mapeo(fila) for fila in datos_extraidos]
                
                self.ventana.after(0, lambda t=tabla, p=progreso_general: self.actualizar_ui_tabla(
                    t, p, 85, "cargando"))
                
                # Cargar en destino
                config_carga = {"tamano_lote_carga": 500, "usar_upsert": True}
                cargador = CargadorMasivo(self.conector_destino, config_carga)
                cargados = cargador.cargar_lote(datos_transformados, tabla)
                
                self.metricas["cargados"] += cargados
                self.metricas["tablas_migradas"] += 1
                
                self.ventana.after(0, lambda t=tabla, p=progreso_general: self.actualizar_ui_tabla(
                    t, p, 100, f"completada ({cargados} registros)"))
                
                self.ventana.after(0, self.actualizar_estadisticas)
                self.log(f"  Tabla '{tabla}' migrada: {cargados} registros")
                
            except Exception as e:
                self.metricas["errores"] += 1
                self.metricas["tablas_con_error"].append(tabla)
                self.log(f"  ERROR en tabla '{tabla}': {str(e)}")
                self.ventana.after(0, self.actualizar_estadisticas)
        
        # Finalizar
        self.proceso_activo = False
        self.ventana.after(0, self.finalizar_migracion)
    
    def actualizar_ui_tabla(self, tabla, progreso_general, progreso_tabla, estado):
        self.barra_progreso['value'] = progreso_general
        self.label_porcentaje.config(text=f"{progreso_general}%")
        self.barra_tabla['value'] = progreso_tabla
        self.label_tabla_actual.config(text=f"Tabla actual: {tabla} - {estado}")
        self.label_estado.config(text=f"Migrando tabla {tabla}...")
    
    def actualizar_estadisticas(self):
        self.stat_tablas_migradas.config(text=str(self.metricas["tablas_migradas"]))
        self.stat_extraidos.config(text=str(self.metricas["extraidos"]))
        self.stat_cargados.config(text=str(self.metricas["cargados"]))
        self.stat_errores.config(text=str(self.metricas["errores"]))
    
    def finalizar_migracion(self):
        self.barra_progreso['value'] = 100
        self.label_porcentaje.config(text="100%")
        self.barra_tabla['value'] = 100
        self.label_tabla_actual.config(text="")
        self.label_estado.config(text="Migracion completada")
        self.btn_iniciar.config(state='normal', bg='#4CAF50')
        self.btn_pausar.config(state='disabled')
        
        self.log("\n" + "=" * 60)
        self.log("MIGRACION COMPLETADA")
        self.log(f"  Tablas migradas: {self.metricas['tablas_migradas']}/{len(self.tablas_a_migrar)}")
        self.log(f"  Total registros extraidos: {self.metricas['extraidos']}")
        self.log(f"  Total registros cargados: {self.metricas['cargados']}")
        self.log(f"  Errores: {self.metricas['errores']}")
        
        if self.metricas["tablas_con_error"]:
            self.log(f"  Tablas con error: {', '.join(self.metricas['tablas_con_error'])}")
        
        self.log("=" * 60)
        
        self.barra_estado.config(text="  Migracion finalizada")
        
        messagebox.showinfo("Migracion Completada",
                          f"Proceso finalizado.\n\n"
                          f"Tablas migradas: {self.metricas['tablas_migradas']}/{len(self.tablas_a_migrar)}\n"
                          f"Registros: {self.metricas['cargados']}\n"
                          f"Errores: {self.metricas['errores']}")
    
    def pausar_migracion(self):
        self.proceso_pausado = not self.proceso_pausado
        if self.proceso_pausado:
            self.btn_pausar.config(text="REANUDAR")
            self.log("Migracion pausada")
            self.label_estado.config(text="PAUSADO")
        else:
            self.btn_pausar.config(text="PAUSAR")
            self.log("Migracion reanudada")
            self.label_estado.config(text="Migrando...")
    
    def ver_reporte(self):
        ventana_reporte = tk.Toplevel(self.ventana)
        ventana_reporte.title("Reporte de Migracion")
        ventana_reporte.geometry("650x550")
        ventana_reporte.configure(bg='white')
        
        tk.Label(ventana_reporte, text="REPORTE DE MIGRACION DE BASE DE DATOS",
                font=('Segoe UI', 14, 'bold'), fg='#333333', bg='white').pack(pady=20)
        
        frame_detalle = ttk.Frame(ventana_reporte)
        frame_detalle.pack(fill='x', padx=30, pady=10)
        
        ttk.Label(frame_detalle, text=f"Origen:  {self.bd_origen.get()} - {self.nombre_bd_origen.get()}",
                 font=('Segoe UI', 10)).pack(anchor='w', pady=2)
        ttk.Label(frame_detalle, text=f"Destino: {self.bd_destino.get()} - {self.nombre_bd_destino.get()}",
                 font=('Segoe UI', 10)).pack(anchor='w', pady=2)
        ttk.Label(frame_detalle, text=f"Fecha:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                 font=('Segoe UI', 10)).pack(anchor='w', pady=2)
        
        ttk.Separator(ventana_reporte, orient='horizontal').pack(fill='x', padx=30, pady=10)
        
        texto = scrolledtext.ScrolledText(ventana_reporte, height=15, bg='white', fg='#333333',
                                          font=('Consolas', 10))
        texto.pack(fill='both', expand=True, padx=30, pady=10)
        
        reporte = f"""RESUMEN DE MIGRACION
{'='*50}

Tablas encontradas en origen: {len(self.tablas_a_migrar)}
Tablas migradas exitosamente: {self.metricas.get('tablas_migradas', 0)}

ESTADISTICAS:
  Total registros extraidos:  {self.metricas.get('extraidos', 0)}
  Total registros cargados:   {self.metricas.get('cargados', 0)}
  Errores encontrados:       {self.metricas.get('errores', 0)}

TABLAS EN BASE DE DATOS ORIGEN:
"""
        for tabla in self.tablas_a_migrar:
            info = self.esquema_descubierto["tablas"][tabla] if self.esquema_descubierto else {}
            num_cols = len(info.get("columnas", []))
            reporte += f"  - {tabla} ({num_cols} columnas)\n"
        
        if self.metricas.get("tablas_con_error"):
            reporte += f"\nTABLAS CON ERROR:\n"
            for tabla in self.metricas["tablas_con_error"]:
                reporte += f"  - {tabla}\n"
        
        texto.insert('1.0', reporte)
        texto.config(state='disabled')
        
        ttk.Button(ventana_reporte, text="Cerrar", 
                  command=ventana_reporte.destroy).pack(pady=10)
    
    def exportar_logs(self):
        archivo = filedialog.asksaveasfilename(defaultextension=".log",
                                               filetypes=[("Archivos de log", "*.log"), ("Texto", "*.txt")])
        if archivo:
            with open(archivo, 'w', encoding='utf-8') as f:
                f.write(self.area_logs.get('1.0', 'end'))
            messagebox.showinfo("Exportado", f"Registro exportado a:\n{archivo}")

if __name__ == "__main__":
    ventana = tk.Tk()
    app = AplicacionMigrador(ventana)
    ventana.mainloop()