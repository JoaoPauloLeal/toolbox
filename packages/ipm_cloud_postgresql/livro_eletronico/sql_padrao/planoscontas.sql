SELECT id_orig as key1,
           id_orig as id,
           id_banco,
           mascara,
           descricao,
           plano_vigente
      FROM (select '1' as id_orig,
                   '2' as id_banco,
                    '#.#.#.##.##-#' as mascara,
                   'BANCO DO BRASIL' as descricao,
                    'S' as plano_vigente
           union all
            select '756' as id_orig,
                   '150' as id_banco,
                    '#.#.#.##.##-#' mascara,
                   'BANCOOB' as descricao,
                    'S' as plano_vigente
           union all
            select '237' as id_orig,
                   '72' as id_banco,
                    '#.#.#.##.##-#' as mascara,
                   'BRADESCO' as descricao,
                    'S' as plano_vigente
           union all
            select '133' as id_orig,
                   '531' as id_banco,
                    '#.#.#.##.##-#' as mascara,
                   'CONFEDERACAO' as descricao,
                    'S' as plano_vigente
           union all
            select '104' as id_orig,
                   '54' as id_banco,
                    '#.#.#.##.##-#' as mascara,
                   'CAIXA ECONOMICA' as descricao,
                    'S' as plano_vigente
           ) as plano_cta
