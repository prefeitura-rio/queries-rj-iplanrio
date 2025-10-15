import yaml

def get_all_keys(d, nivel=0):
    """
    Função geradora que percorre recursivamente um dicionário
    e retorna cada chave com seu nível de profundidade na árvore.
    
    Args:
        d: Dicionário para percorrer
        nivel: Nível atual na árvore (inicia em 0)
    
    Yields:
        tuple: (chave, nivel) para cada chave encontrada
    """
    for key, value in d.items():
        yield key, nivel
        if isinstance(value, dict):
            yield from get_all_keys(value, nivel + 1)

def print_tree_structure(data):
    """
    Imprime a estrutura hierárquica da seção models.queries do YAML.
    
    Args:
        data: Dados carregados do YAML
    """
    # Navega até a seção models.queries
    current_data = data['models']['queries']
    
    print("Estrutura da seção 'models.queries':")
    print("-" * 50)
    
    for key, nivel in get_all_keys(current_data):
        indentacao = "  " * nivel  # 2 espaços por nível
        print(f"{indentacao}{key} (nível {nivel})")

def check_resource_tags(data):
    """
    Verifica quais chaves de nível 2 não possuem +resource_tags no nível 3.
    
    Args:
        data: Dados carregados do YAML
    
    Returns:
        list: Lista de chaves de nível 2 que não possuem +resource_tags no nível 3
    """
    # Navega até a seção models.queries
    current_data = data['models']['queries']
    
    # Coleta todas as chaves de nível 2 e verifica se têm +resource_tags no nível 3
    level2_keys_without_resource_tags = []
    
    def check_level2_keys(d, current_path="", current_level=0):
        for key, value in d.items():
            if isinstance(value, dict):
                # Se estamos no nível 1, verifica os filhos (nível 2)
                if current_level == 1:
                    for subkey, subvalue in value.items():
                        if isinstance(subvalue, dict):
                            # Esta é uma chave de nível 2 - verifica se tem +resource_tags
                            has_resource_tags = '+resource_tags' in subvalue
                            
                            if not has_resource_tags:
                                full_path = f"{current_path}.{key}.{subkey}" if current_path else f"{key}.{subkey}"
                                level2_keys_without_resource_tags.append(full_path)
                
                # Continua recursivamente
                new_path = f"{current_path}.{key}" if current_path else key
                check_level2_keys(value, new_path, current_level + 1)
    
    check_level2_keys(current_data, "", 0)
    return level2_keys_without_resource_tags

if __name__ == "__main__":
    with open('dbt_project.yml', 'r') as file:
        data = yaml.safe_load(file)
    
    # Mostra a estrutura da seção models.queries
    # print_tree_structure(data)
    
    print("\n" + "="*60)
    print("VERIFICAÇÃO DE +resource_tags")
    print("="*60)
    
    # Verifica quais chaves de nível 2 não têm +resource_tags no nível 3
    missing_resource_tags = check_resource_tags(data)
    
    if missing_resource_tags:
        print(f"\n❌ Encontrado {len(missing_resource_tags)} projetos SEM Resource Tags:")
        for i, key in enumerate(missing_resource_tags, 1):
            print(f"  {i}. {key}")
    else:
        print("\n✅ Todas os projetos possuem Resource Tags!")
    
    # Mostra um resumo detalhado
    print(f"\n📊 RESUMO DETALHADO:")
    print(f"   • Total de projetos analisados: {len([k for k, v in get_all_keys(data['models']['queries']) if v == 2])}")
    print(f"   • Projetos COM Resource Tags: {len([k for k, v in get_all_keys(data['models']['queries']) if v == 2]) - len(missing_resource_tags)}")
    print(f"   • Projetos SEM Resource Tags: {len(missing_resource_tags)}")
    