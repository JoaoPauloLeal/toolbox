function mostraModal(idModal) {
  var display = document.querySelector(`#${idModal}`).style.display;
  if (display === "none") {
    var display = (document.querySelector(`#${idModal}`).style.display =
      "block");
  }
}

function escondeModal(idModal) {
  document.querySelector(`#${idModal}`).style.display = "none";
}

function abreDivCronograma() {
  document.querySelector("#cronograma").style.display = "block";
  document.querySelector("#pesquisa").style.display = "none";
}

function redirect(tela) {
  window.location.replace(tela);
  return false;
}

function abreDivPesquisa() {
  document.querySelector("#cronograma").style.display = "none";
  document.querySelector("#pesquisa").style.display = "block";
}

function enviarPesquisa() {
  var nome = $("#formNome").val();
  var pergunta1 = $("#formPergunta1")
    .find(":selected")
    .val();
  var pergunta2 = $("#formPergunta2")
    .find(":selected")
    .val();
  var nota = $("#formNota")
    .find(":selected")
    .val();
  var comentario = $("#formComentario").val();

  if (!nome) {
    alert("É necessário informar o nome");
    return;
  }

  $.post(
    "https://pesquisasatisfacao.herokuapp.com/",
    {
      nome: nome,
      pergunta1: pergunta1,
      pergunta2: pergunta2,
      nota: nota,
      comentario: comentario
    },
    function(result) {
      if (result === "Sucesso!") {
        alert(result);
        $("#formNome").val("");
        $("#formComentario").val("");
        $("#formPergunta1").prop('selectedIndex',0);
        $("#formPergunta2").prop('selectedIndex',0);
        $("#formNota").prop('selectedIndex',0);
      } else {
        alert("Erro");
      }
    }
  );
}
